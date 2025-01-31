"""This file contains the function that returns the needed benchmark"""
from cctest_core import smallbank_module
from cctest_core.protocol import Benchmark


def get_benchmark(name: str) -> Benchmark:
    """Returns the benchmark instance for a given name"""
    if name == "smallBank":
        return smallbank_module.SmallBank()
    raise ValueError(f"Unknown benchmark: {name}")
