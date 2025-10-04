"""Run database migrations for Pulsar Neuron."""
from __future__ import annotations

from pulsar_neuron.db.postgres import migrate


def main() -> None:
    migrate()
    print("✅ migrations applied")


if __name__ == "__main__":
    main()
