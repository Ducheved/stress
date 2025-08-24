```
python3 stress.py \
  --mem 8Gi --block 64Mi --mem-interval 2 --headroom 256Mi \
  --cpus 2 --cpu-duration 600 --cpu-ramp-every 15 --duty-on 700 --duty-off 300 \
  --logfile ./stress.log
```

