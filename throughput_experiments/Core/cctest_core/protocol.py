"""File that specifies the protocol for the Benchmarks"""
import random
from abc import abstractmethod
from typing import Optional, Protocol
from dataclasses import dataclass
import psycopg2 as pg


@dataclass
class TransactionResult:
    """Dataclass collecting the result of running a transaction"""
    program_name: str
    isolation_level: str
    total_time: float = 0.0
    num_deadlock_abort: int = 0
    num_conc_abort: int = 0
    num_serial_abort: int = 0


class Benchmark(Protocol):
    """Protocol for the benchmarks"""
    @abstractmethod
    def init_db(self, config_dict: dict):
        """Initializes the database"""
        raise NotImplementedError

    @abstractmethod
    def run_transact(self,
                     config_dict: dict,
                     conn: pg.extensions.connection,
                     process_id: Optional[int] = None,
                     logfile: Optional[str] = None) -> TransactionResult:
        """Runs a transaction in the benchmark based on the given config file"""
        raise NotImplementedError

    @abstractmethod
    def check_config(self, config_dict: dict):
        """Checks if the config file is valid for the benchmark"""
        raise NotImplementedError

    @abstractmethod
    def check_consistency(self, config):
        """Checks if the database is consistent"""
        raise NotImplementedError

    def zipfian(self, skew: float, n: int) -> int:
        """Sample an account from the database by the zipfian sampling method"""
        s: float = 0.0
        for i in range(n):
            s += 1 / (i+1)**skew
        value = random.random() * s
        s = 0.0
        for i in range(n):
            s += 1 / (i+1)**skew
            if s > value:
                return i+1
        return n
