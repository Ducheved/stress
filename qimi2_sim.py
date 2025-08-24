import argparse, multiprocessing as mp, os, sys, time, signal, tempfile, math

PAGE_SIZE = 4096

def parse_size(s: str) -> int:
    s = s.strip().lower()
    units = {'k':1024,'ki':1024,'m':1024**2,'mi':1024**2,'g':1024**3,'gi':1024**3,'t':1024**4,'ti':1024**4}
    for u, mul in sorted(units.items(), key=lambda x: -len(x[0])):
        if s.endswith(u): return int(float(s[:-len(u)]) * mul)
    return int(s)

def human(n: int | None) -> str:
    if n is None: return "n/a"
    f = float(n)
    for u in ["B","KiB","MiB","GiB","TiB"]:
        if f < 1024 or u == "TiB": return f"{f:.2f} {u}"
        f /= 1024

class DualLogger:
    def __init__(self, path: str):
        self.f = open(path, "a", buffering=1, encoding="utf-8")
        self.log(f"=== start pid={os.getpid()} at {time.strftime('%F %T')} ===")
    def log(self, msg: str):
        line = f"{time.strftime('%F %T')} | {msg}\n"
        try:
            sys.stdout.write(line); sys.stdout.flush()
            self.f.write(line); self.f.flush(); os.fsync(self.f.fileno())
        except Exception: pass
    def close(self):
        try: self.log("=== graceful stop ==="); self.f.close()
        except Exception: pass

def read_cgroup_limits():
    mem_limit=None; cpu_quota=None; cpu_period=None
    try:
        with open("/sys/fs/cgroup/memory.max") as f:
            v=f.read().strip(); mem_limit=None if v=="max" else int(v)
    except Exception: pass
    if mem_limit is None:
        for p in ("/sys/fs/cgroup/memory/memory.limit_in_bytes","/sys/fs/cgroup/memory.limit_in_bytes"):
            try:
                with open(p) as f: mem_limit=int(f.read().strip()); break
            except Exception: pass
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            a=f.read().strip().split()
            if len(a)==2 and a[0]!="max": cpu_quota=int(a[0]); cpu_period=int(a[1])
    except Exception: pass
    if cpu_quota is None or cpu_period is None:
        try:
            with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") as f1: q=int(f1.read().strip())
            with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us") as f2: p=int(f2.read().strip())
            if q>0: cpu_quota, cpu_period = q, p
        except Exception: pass
    return mem_limit, cpu_quota, cpu_period

def read_mem_current():
    try:
        with open("/sys/fs/cgroup/memory.current") as f: return int(f.read().strip())
    except Exception: pass
    for p in ("/sys/fs/cgroup/memory/memory.usage_in_bytes","/sys/fs/cgroup/memory.usage_in_bytes"):
        try:
            with open(p) as f: return int(f.read().strip())
        except Exception: pass
    return None

def read_mem_peak():
    try:
        with open("/sys/fs/cgroup/memory.peak") as f: return int(f.read().strip())
    except Exception: pass
    for p in ("/sys/fs/cgroup/memory/memory.max_usage_in_bytes","/sys/fs/cgroup/memory.max_usage_in_bytes"):
        try:
            with open(p) as f: return int(f.read().strip())
        except Exception: pass
    return None

def read_cpu_stat_v2():
    d={}
    try:
        with open("/sys/fs/cgroup/cpu.stat") as f:
            for line in f:
                k,v=line.strip().split()
                d[k]=int(v)
    except Exception:
        pass
    return d

def touch_pages(buf: bytearray):
    for off in range(0, len(buf), PAGE_SIZE): buf[off]=1

def mem_burst(logger: DualLogger, want_bytes: int, block_bytes: int, headroom_bytes: int):
    blocks=[]; allocated=0
    lim,_,_ = read_cgroup_limits()
    logger.log(f"[mem] burst target={human(want_bytes)} block={human(block_bytes)} headroom={human(headroom_bytes)} limit={human(lim)}")
    try:
        while allocated < want_bytes:
            cur = read_mem_current()
            if lim is not None and cur is not None and cur >= max(0, lim - headroom_bytes):
                logger.log(f"[mem] stop before headroom: cur={human(cur)} / limit={human(lim)}")
                break
            remain = want_bytes - allocated
            bsz = min(block_bytes, remain)
            logger.log(f"[mem] plan +{human(bsz)} (now={human(allocated)})")
            blk = bytearray(bsz); touch_pages(blk)
            blocks.append(blk); allocated += bsz
            cur = read_mem_current(); peak=read_mem_peak()
            ratio = f"{(cur/lim*100):.1f}%" if (cur and lim) else "n/a"
            logger.log(f"[mem] allocated={human(allocated)} cur={human(cur)} peak={human(peak)} of limit {human(lim)} ({ratio})")
    except MemoryError:
        logger.log(f"[mem] MemoryError at {human(allocated)} / {human(want_bytes)}")
    return blocks

def cpu_worker(stop_at: float, pin_cpu):
    try:
        if pin_cpu is not None and hasattr(os, "sched_setaffinity"):
            os.sched_setaffinity(0, {pin_cpu})
    except Exception: pass
    x=0.0
    while time.time()<stop_at:
        x=(x+1.0000001)*1.0000002
        if x>1e12: x%=123456.789

def cpu_burst(logger: DualLogger, nproc: int, duration_s: int, no_affinity: bool):
    stop_at = time.time()+max(1,duration_s)
    try:
        avail = sorted(os.sched_getaffinity(0))
    except Exception:
        avail=None
    procs=[]
    for i in range(nproc):
        pin=None
        if not no_affinity and avail: pin = avail[i % len(avail)]
        p=mp.Process(target=cpu_worker, args=(stop_at, pin))
        p.daemon=True
        p.start(); procs.append(p)
        logger.log(f"[cpu] started worker {i+1}/{nproc} pin={pin}")
    for p in procs: p.join()

def io_burst(logger: DualLogger, size_bytes: int, dir_path: str):
    if size_bytes <= 0: 
        logger.log("[io] skipped")
        return
    os.makedirs(dir_path, exist_ok=True)
    fd, path = tempfile.mkstemp(prefix="qimi2_", suffix=".bin", dir=dir_path)
    logger.log(f"[io] writing {human(size_bytes)} to {path}")
    try:
        with os.fdopen(fd, "wb", buffering=0) as f:
            chunk = bytes(1024*1024)  # 1MiB zeros
            left = size_bytes
            while left > 0:
                n = min(left, len(chunk))
                f.write(chunk[:n])
                left -= n
            f.flush(); os.fsync(f.fileno())
        logger.log("[io] done")
    except Exception as e:
        logger.log(f"[io] error: {e}")
    finally:
        try:
            os.remove(path)
            logger.log("[io] temp file removed")
        except Exception: pass

def main():
    ap=argparse.ArgumentParser(description="Fast qimi2-like startup spike (mem+cpu+io) with logging")
    ap.add_argument("--mem-burst", default="2Gi", help="Сколько памяти быстро занять (по умолчанию 2Gi)")
    ap.add_argument("--mem-block", default="128Mi", help="Размер одного блока аллокации (по умолчанию 128Mi)")
    ap.add_argument("--headroom", default="256Mi", help="Не заходить ближе чем на headroom к лимиту")
    ap.add_argument("--cpus", type=int, default=2, help="Сколько CPU-воркеров (по умолчанию 2)")
    ap.add_argument("--duration", type=int, default=30, help="Общая длительность CPU-фазы в секундах (по умолчанию 30)")
    ap.add_argument("--io-size", default="128Mi", help="Сколько записать во временный файл (0 чтобы выключить)")
    ap.add_argument("--io-dir", default="/tmp", help="Куда писать временный файл")
    ap.add_argument("--no-affinity", action="store_true", help="Не пиновать воркеры к ядрам")
    ap.add_argument("--logfile", default="./qimi2_sim.log", help="Файл логов (на PVC)")
    args=ap.parse_args()

    logger=DualLogger(args.logfile)
    def on_term(sig, frm):
        logger.log(f"[main] signal {sig}, exit"); logger.close(); sys.exit(0)
    for sig in (signal.SIGINT, signal.SIGTERM): signal.signal(sig, on_term)

    mem_limit, cpu_quota, cpu_period = read_cgroup_limits()
    eff_cpu = (cpu_quota/cpu_period) if (cpu_quota and cpu_period) else None
    logger.log("=== cgroup limits ===")
    logger.log(f"memory.max: {human(mem_limit)}")
    logger.log(f"cpu.max: {cpu_quota}/{cpu_period} (~{eff_cpu:.2f} CPUs)" if eff_cpu else "cpu.max: unlimited/unknown")
    logger.log("=====================")

    cpu0 = read_cpu_stat_v2()

    blocks = mem_burst(
        logger,
        want_bytes=parse_size(args.mem_burst),
        block_bytes=parse_size(args.mem_block),
        headroom_bytes=parse_size(args.headroom),
    )

    io_size = parse_size(args.io_size)
    io_proc = mp.Process(target=io_burst, args=(logger, io_size, args.io_dir))
    io_proc.daemon=True
    io_proc.start()

    cpu_burst(logger, nproc=max(1,args.cpus), duration_s=max(1,args.duration), no_affinity=args.no_affinity)

    io_proc.join(timeout=1.0)

    cur = read_mem_current(); peak=read_mem_peak()
    cpu1 = read_cpu_stat_v2()
    if cpu0 and cpu1 and "throttled_usec" in cpu1:
        thr = cpu1.get("throttled_usec",0) - cpu0.get("throttled_usec",0)
        nper = cpu1.get("nr_periods",0) - cpu0.get("nr_periods",0)
        nthr = cpu1.get("nr_throttled",0) - cpu0.get("nr_throttled",0)
        logger.log(f"[final] mem.current={human(cur)} peak={human(peak)}; throttled_usec+={thr} nr_throttled+={nthr} periods+={nper}")
    else:
        logger.log(f"[final] mem.current={human(cur)} peak={human(peak)}; cpu.stat v2 not available")

    logger.close()

if __name__=="__main__":
    main()
