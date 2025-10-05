from __future__ import annotations
import signal, sys, time, threading


# Example placeholders; replace with real jobs:
def run_fut_oi_once():
    # from pulsar_neuron.ingest.fut_oi_job import run as job_run
    # job_run(mode="live")
    pass


def run_options_once():
    # from pulsar_neuron.ingest.options_job import run as job_run
    # job_run(mode="live", strikes=5)
    pass


def run_breadth_once():
    # from pulsar_neuron.ingest.market_job import run as job_run
    # job_run(mode="live")
    pass


def main():
    stop = threading.Event()

    def loop(name, fn, interval):
        while not stop.is_set():
            t0 = time.time()
            try:
                fn()
            except Exception as e:
                print(f"[scheduler] {name} error: {e}", file=sys.stderr)
            dt = time.time() - t0
            sleep_for = max(0.0, interval - dt)
            stop.wait(sleep_for)

    threads = [
        threading.Thread(target=loop, args=("fut_oi", run_fut_oi_once, 120), daemon=True),
        threading.Thread(target=loop, args=("options", run_options_once, 180), daemon=True),
        threading.Thread(target=loop, args=("breadth", run_breadth_once, 300), daemon=True),
    ]
    for th in threads:
        th.start()

    def _shutdown(*_):
        stop.set()
        for th in threads:
            th.join(timeout=2.0)
        print("[scheduler] shutdown complete")
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # keep main alive
    while not stop.is_set():
        stop.wait(1.0)


if __name__ == "__main__":
    main()
