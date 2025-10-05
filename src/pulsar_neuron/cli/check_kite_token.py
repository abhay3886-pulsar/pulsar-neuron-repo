from __future__ import annotations
import time
from pathlib import Path
from pulsar_neuron.config.kite_auth import load_kite_creds, LOCAL_TOKEN_FILE


def main():
    creds = load_kite_creds()
    ak = creds.get("api_key")
    at = creds.get("access_token")
    print("api_key      :", (ak[:6] + "…") if ak else None)
    print("access_token :", (at[:6] + "…") if at else None)

    p = Path(LOCAL_TOKEN_FILE)
    if p.exists():
        age = time.time() - p.stat().st_mtime
        print(f"token_file   : {p} (age ~{int(age)}s)")
    else:
        print(f"token_file   : {p} (missing; probably using AWS Secrets)")


if __name__ == "__main__":
    main()
