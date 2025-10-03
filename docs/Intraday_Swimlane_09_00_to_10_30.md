# Intraday Swimlane (09:00–10:30 IST)
- 09:00 Warm-up; start ingest jobs
- 09:15 Market open (first 5m bar running)
- 09:20 First 5m bar closes (~09:20:10) → context pack published
- 09:20 Capture Futures OI baseline (write-once)
- 09:22 Options chain refresh
- 10:15 IB snapshot sealed; ORB hold/fake determined
- Agent ticks: 30–60s (IB), 60–90s (mid), 60s (late)
