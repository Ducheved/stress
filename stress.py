import argparse
import multiprocessing as mp
import os
import sys
import time
import signal

PAGE_SIZE = 4096

def parse_size(s: str) -> int:
    s = s.strip().lower()
    units = {'k':1024,'ki':1024,'m':1024**2,'mi':1024**2,'g':1024**3,'gi':1024**3,'t':1024**4,'ti':1024**4}
    for u, mul in sorted(units.items(), key=lambda x: -len(x[0])):
        if s.endswith(u):
            return int(float(s[:-len(u)]) * mul)
    return int(s)

def human(n: int) -> str:
    n = float(n)
    for unit in ["B","KiB","MiB","GiB","TiB"]:
        if n < 1024 or unit == "TiB":
            return f"{n:.2f} {unit}"
        n /= 1024

class DualLogger:
    def __init__(self, path: str):
        self.path = path
        self.f = open(path, "a", buffering=1, encoding="utf-8")
        self.log(f"=== start pid={os.getpid()} at {time.strftime('%F %T')} ===")

    def log(self, msg: str):
        line = f"{time.strftime('%F %T')} | {msg}\n"
        try:
            sys.stdout.write(line); sys.stdout.flush()
            self.f.write(line); self.f.flush(); os.fsync(self.f.fileno())
        except Exception:
            pass

    def close(self):
        try:
            self.log("=== graceful stop ===")
            self.f.close()
        except Exception:
            pass

def read_cgroup_limits():
    mem_limit = None; cpu_quota = None; cpu_period = None
    try:
        with open("/sys/fs/cgroup/memory.max","r") as f:
            v = f.read().strip(); mem_limit = None if v == "max" else int(v)
    except Exception: pass
    if mem_limit is None:
        for p in ("/sys/fs/cgroup/memory/memory.limit_in_bytes","/sys/fs/cgroup/memory.limit_in_bytes"):
            try:
                with open(p,"r") as f: mem_limit = int(f.read().strip()); break
            except Exception: pass
    try:
        with open("/sys/fs/cgroup/cpu.max","r") as f:
            parts = f.read().strip().split()
            if len(parts)==2 and parts[0]!="max": cpu_quota=int(parts[0]); cpu_period=int(parts[1])
    except Exception: pass
    if cpu_quota is None or cpu_period is None:
        try:
            with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us","r") as f1: q = int(f1.read().strip())
            with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us","r") as f2: p = int(f2.read().strip())
            if q > 0: cpu_quota, cpu_period = q, p
        except Exception: pass
    return mem_limit, cpu_quota, cpu_period

def read_mem_current():
    try:
        with open("/sys/fs/cgroup/memory.current","r") as f: return int(f.read().strip())
    except Exception: pass
    for p in ("/sys/fs/cgroup/memory/memory.usage_in_bytes","/sys/fs/cgroup/memory.usage_in_bytes"):
        try:
            with open(p,"r") as f: return int(f.read().strip())
        except Exception: pass
    return None

def read_mem_peak():
    try:
        with open("/sys/fs/cgroup/memory.peak","r") as f: return int(f.read().strip())
    except Exception: pass
    for p in ("/sys/fs/cgroup/memory/memory.max_usage_in_bytes","/sys/fs/cgroup/memory.max_usage_in_bytes"):
        try:
            with open(p,"r") as f: return int(f.read().strip())
        except Exception: pass
    return None

def read_mem_events_v2():
    d={}
    for fname in ("memory.events.local","memory.events"):
        try:
            with open(f"/sys/fs/cgroup/{fname}","r") as f:
                for line in f:
                    k, v = line.strip().split(); d[k]=int(v)
            break
        except Exception: continue
    return d

def read_self_rss():
    try:
        with open("/proc/self/status","r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    kb = int(line.split()[1]); return kb*1024
    except Exception: pass
    return None

def touch_pages(buf: bytearray):
    for off in range(0, len(buf), PAGE_SIZE): buf[off]=1

def allocate_slow(logger: DualLogger, total_bytes: int, block_bytes: int, pause_sec: float, headroom_bytes: int):
    blocks=[]; allocated=0; start=time.time()
    logger.log(f"[mem] target={human(total_bytes)}, block={human(block_bytes)}, pause={pause_sec:.2f}s, headroom={human(headroom_bytes)}")
    try:
        while allocated < total_bytes:
            cur = read_mem_current(); lim,_,_ = read_cgroup_limits()
            if cur is not None and lim is not None and cur >= max(0, lim - headroom_bytes):
                logger.log(f"[mem] stopping before headroom breach: mem.current={human(cur)} / limit={human(lim)}")
                break
            remaining = total_bytes - allocated
            bsz = min(block_bytes, remaining)
            logger.log(f"[mem] plan: +{human(bsz)} next (allocated={human(allocated)}) cur={human(cur) if cur else 'n/a'}")
            blk = bytearray(bsz); touch_pages(blk)
            blocks.append(blk); allocated += bsz
            cur = read_mem_current(); peak=read_mem_peak(); rss=read_self_rss(); ev=read_mem_events_v2()
            ratio = f"{(cur/lim*100):.1f}%" if (cur is not None and lim) else "n/a"
            logger.log(f"[mem] allocated={human(allocated)} blocks={len(blocks)} cgroup.current={human(cur) if cur else 'n/a'} "
                       f"peak={human(peak) if peak else 'n/a'} rss={human(rss) if rss else 'n/a'} limit={human(lim) if lim else 'n/a'} ({ratio}) events={ev if ev else {}}")
            time.sleep(pause_sec)
    except MemoryError:
        logger.log(f"[mem] MemoryError at {human(allocated)} / requested {human(total_bytes)}")
    dur=time.time()-start
    logger.log(f"[mem] done: {human(allocated)} in {dur:.2f}s blocks={len(blocks)}")
    return blocks

def cpu_worker(stop_at: float, duty_on_ms: int, duty_off_ms: int, pin_cpu):
    try:
        if pin_cpu is not None and hasattr(os, "sched_setaffinity"): os.sched_setaffinity(0, {pin_cpu})
    except Exception: pass
    x=0.0; on=duty_on_ms/1000.0; off=duty_off_ms/1000.0
    while time.time() < stop_at:
        t_end = time.time() + on
        while time.time() < t_end:
            x = (x + 1.0000001) * 1.0000002
            if x > 1e12: x = x % 123456.789
        if off > 0: time.sleep(off)

def run_cpu_ramp(logger: DualLogger, total_cpus: int, duration: int, ramp_every: float, duty_on_ms: int, duty_off_ms: int, no_affinity: bool):
    stop_at = time.time() + max(1, duration)
    try:
        available = sorted(os.sched_getaffinity(0))
    except Exception:
        available = None
    workers=[]; started=0
    while time.time() < stop_at:
        if started < total_cpus:
            pin = None
            if not no_affinity and available: pin = available[started % len(available)]
            p = mp.Process(target=cpu_worker, args=(stop_at, duty_on_ms, duty_off_ms, pin))
            p.daemon = True
            p.start()
            workers.append(p)
            started += 1
            logger.log(f"[cpu] worker started={started}/{total_cpus} pin={pin} duty={duty_on_ms}ms/{duty_off_ms}ms")
            time.sleep(ramp_every)
        else:
            time.sleep(0.5)
    for p in workers:
        p.join(timeout=1.0)

def main():
    ap = argparse.ArgumentParser(description="Slow-ramp stress for RAM/CPU with durable logging")
    ap.add_argument("--mem", default="8Gi")
    ap.add_argument("--block", default="64Mi")
    ap.add_argument("--mem-interval", type=float, default=2.0)
    ap.add_argument("--headroom", default="0")
    ap.add_argument("--cpus", type=int, default=2)
    ap.add_argument("--cpu-duration", type=int, default=300)
    ap.add_argument("--cpu-ramp-every", type=float, default=15.0)
    ap.add_argument("--duty-on", type=int, default=700)
    ap.add_argument("--duty-off", type=int, default=300)
    ap.add_argument("--no-affinity", action="store_true")
    ap.add_argument("--logfile", default="./stress.log")
    args = ap.parse_args()

    logger = DualLogger(args.logfile)

    def on_term(sig, frm):
        logger.log(f"[main] received signal {sig}, exiting"); logger.close(); sys.exit(0)
    for sig in (signal.SIGINT, signal.SIGTERM): signal.signal(sig, on_term)

    total_bytes = parse_size(args.mem); block_bytes = parse_size(args.block); headroom_bytes = parse_size(args.headroom)
    mem_limit, cpu_quota, cpu_period = read_cgroup_limits()
    eff_cpu = (cpu_quota / cpu_period) if (cpu_quota and cpu_period) else None
    logger.log("=== cgroup limits ===")
    logger.log(f"memory.max: {human(mem_limit) if mem_limit else 'unlimited/unknown'}")
    logger.log(f"cpu.max: {cpu_quota}/{cpu_period} (~{eff_cpu:.2f} CPUs)" if eff_cpu else "cpu.max: unlimited/unknown")
    logger.log("=====================")

    cpu_proc = mp.Process(target=run_cpu_ramp, args=(
        logger, max(1, args.cpus), args.cpu_duration, args.cpu_ramp_every,
        max(1, args.duty_on), max(0, args.duty_off), args.no_affinity,
    ))
    cpu_proc.start()
    logger.log(f"[cpu] ramp started pid={cpu_proc.pid}")

    blocks = allocate_slow(logger, total_bytes, block_bytes, max(0.0, args.mem_interval), headroom_bytes)

    cpu_proc.join()
    cur=read_mem_current(); peak=read_mem_peak(); ev=read_mem_events_v2(); rss=read_self_rss()
    logger.log(f"[final] cgroup.current={human(cur) if cur else 'n/a'} peak={human(peak) if peak else 'n/a'} rss={human(rss) if rss else 'n/a'} events={ev if ev else {}}")
    logger.close()

if __name__ == "__main__":
    main()
