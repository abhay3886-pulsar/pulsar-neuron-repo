"""Replay CLI entry point (stub)."""

from __future__ import annotations

import click


@click.command()
@click.option("--symbol", type=click.Choice(["NIFTY", "BANKNIFTY"]), required=True)
@click.option("--date", "date_str", type=str, required=True, help="Trading session date")
def main(symbol: str, date_str: str) -> None:
    """Emit a friendly replay stub message."""

    click.echo(f"Replay NOT_IMPLEMENTED for {symbol} on {date_str}.")


if __name__ == "__main__":  # pragma: no cover
    main()
