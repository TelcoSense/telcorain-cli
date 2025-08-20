# telcorain-cli

TelcorRain CLI for raingrids calculation. It computes raingrids from all available CMLs and stores them locally, metadata and info in MariaDB, and rainrates in InfluxDB.
It runs and stores data from "current_time – realtime_timewindow", computes it with step "step" each "repetition_interval".

## How to run
Activate the virtual env and install requirements.txt, e.g.:

```
pip install -r requirements.txt
```

Set the configs: start the MariaDB and InfluxDB instances and provide IPs and passwords/tokens.

Run the calculation:
```
python ./telcorain-cli.py --run
```

or wipe all buckets and local output files:
```
python ./telcorain-cli.py --wipe_all
```

## Note
The first iteration will take more than 10 mins.
