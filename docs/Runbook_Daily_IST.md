# Daily Runbook (IST)
## Readiness (08:50)
- NTP in sync, DB reachable, configs loaded, holiday calendar correct

## Pre-open (09:10)
- OHLCV running; bar-completion guard (+10s) active
- 09:20 Futures OI baseline armed
- Option chain ATM ±N coverage

## Mid-session
- context.ok rate ≥ 95%; WAIT reasons mostly 'no setup'

## EOD
- Close positions by policy; roll logs; optional Parquet dump

## EC2 systemd setup
Ensure Pulsar-Neuron live bars wait for the Pulsar-Algo token writer before starting:

```
[Unit]
Description=Pulsar-Neuron Live Bars
After=network-online.target pulsar-token.service
Wants=network-online.target

[Service]
User=ec2-user
WorkingDirectory=/opt/pulsar-neuron-repo
Environment=KITE_TOKEN_FILE=/home/ec2-user/.config/pulsar/kite_token.json
Environment=AWS_REGION=ap-south-1
ExecStart=/home/ec2-user/.local/bin/poetry run python -m pulsar_neuron.cli.live_bars
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```
