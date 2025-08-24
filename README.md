
## stress.py

```
python3 stress.py \
  --mem 8Gi --block 64Mi --mem-interval 2 --headroom 256Mi \
  --cpus 2 --cpu-duration 600 --cpu-ramp-every 15 --duty-on 700 --duty-off 300 \
  --logfile ./stress.log
```

## qimi2_sim.py
```
python3 qimi2_sim.py \
  --mem-burst 6Gi --cpus 2 --duration 30 \
  --io-size 256Mi --headroom 256Mi \
  --logfile ./qimi2_sim.log
```
else
```
python3 qimi2_sim.py --mem-burst 7.5Gi --mem-block 256Mi --headroom 128Mi \
  --cpus 2 --duration 20 --io-size 64Mi --logfile ./qimi2_sim.log
```
else 
```
python3 qimi2_sim.py --mem-burst 6Gi --mem-block 64Mi --headroom 512Mi \
  --cpus 2 --duration 40 --io-size 128Mi --logfile ./qimi2_sim.log
```