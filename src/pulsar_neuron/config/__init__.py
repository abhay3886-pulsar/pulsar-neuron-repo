"""Configuration defaults and helpers for Pulsar Neuron."""

from importlib import resources
from pathlib import Path

__all__ = ["data_path"]


def data_path() -> Path:
    """Return the path to the default configuration directory."""
    return Path(resources.files(__package__))
