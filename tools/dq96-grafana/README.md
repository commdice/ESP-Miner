# DQ96 Grafana dashboard (Umbrel)

Prometheus + Grafana stack that runs on the Umbrel box (192.168.0.27) and keeps 90 days of history for the DQ96 miner
(192.168.0.41) and the Public Pool app it mines to. It reads the miner's `/api/system/info` JSON every 15s, so it works with
the NerdQAxe firmware the DQ96 runs today and with this ESP-Miner fork (which adds the pool health panel). It also checks that
the pool's stratum port (2018) and web UI (2019) answer, which catches the "pool up but no jobs" and "pool app down" failures.

Containers: `prometheus` (storage), `json-exporter` (miner JSON to metrics), `blackbox-exporter` (pool port checks) and
`grafana` (dashboard on port 3300). Only Grafana is exposed on the network.

## Install

Run these on the Umbrel over SSH (`ssh umbrel@192.168.0.27`, using your Umbrel password). The folder goes under
`~/umbrel/home`, the folder the Umbrel Files app shows, which lives on Umbrel's data drive rather than the OS partition that umbrelOS
updates replace.

```bash
mkdir -p ~/umbrel/home/dq96-grafana && cd ~/umbrel/home/dq96-grafana
# copy the contents of this folder here (e.g. from your PC: scp -r tools/dq96-grafana/* umbrel@192.168.0.27:~/umbrel/home/dq96-grafana/)

cp .env.example .env && nano .env          # pick a Grafana admin password
mkdir -p data/prometheus data/grafana
sudo chown 65534:65534 data/prometheus     # Prometheus runs as "nobody"
sudo chown 472:0 data/grafana              # Grafana runs as uid 472
sudo ss -ltn | grep -q ':3300 ' && echo "port 3300 is taken - change it in docker-compose.yml"
sudo docker compose up -d
```

Open `http://192.168.0.27:3300`, log in as `admin` with the password from `.env`; the **DQ96 Lotto Miner** dashboard is the home page.
The containers restart on their own after an Umbrel reboot.

## Day to day

- Update or restart: `cd ~/umbrel/home/dq96-grafana && sudo docker compose pull && sudo docker compose up -d`
- Stop: `sudo docker compose down` (history in `data/` is kept)
- Miner or pool IP changed: edit the targets in `prometheus.yml`, then `sudo docker compose restart prometheus`

## Reading the dashboard

- **Mining** is UP only when the miner answers and reports hashrate. Mining DOWN with **Pool stratum** UP is the classic
  "pool connected but sending no jobs" failure: check the Bitcoin Node app is synced, then restart Public Pool.
- **Pool stratum / Pool web UI** DOWN means the Public Pool app (or its node) on the Umbrel is down.
- **Miner reachable** DOWN means the miner is off the network or rebooting; an **Uptime** drop shows a reboot.
- **Pool health** only fills in when the miner runs the dq96-custom ESP-Miner firmware; on NerdQAxe firmware it shows "No data".
- Share counters reset when the miner reboots or reconnects; the per-hour and 24h figures account for that.
