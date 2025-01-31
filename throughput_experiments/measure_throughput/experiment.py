"""This file is used to run the expirement"""
import argparse
import csv
import os
import json
from multiprocessing import Process, Barrier, Queue
import multiprocessing.synchronize as mps
import time
import sys
from typing import Optional
from dataclasses import dataclass, field
import psycopg2 as pg
from cctest_core.protocol import Benchmark
from cctest_core.benchmarks import get_benchmark

@dataclass
class ProgramResult:
    """Stores the cumulative results for a specific program"""
    isolation_level: str = ""
    total_completed: int = 0
    total_time: float = 0.0
    num_deadlock_abort: int = 0
    num_conc_abort: int = 0
    num_serial_abort: int = 0


@dataclass
class ClientResult:
    """Stores the results of a client process"""
    total_completed: int = 0
    total_il_completed: dict[str, int] = field(default_factory=dict)
    num_deadlock_abort: int = 0
    num_conc_abort: int = 0
    num_serial_abort: int = 0
    programs: dict[str, ProgramResult] = field(default_factory=dict)


def read_config_file(filename):
    """Reads the config file and returns a dictionary"""
    with open(filename, "r", encoding="utf-8") as config_file:
        data = json.load(config_file)
    return data


def write_results(filename, results):
    """Writes the results to a file"""
    with open(filename, "w", encoding="utf-8") as result_file:
        json.dump(results, result_file, indent=4)


def start_processes(config_dict, benchmark_param: Benchmark, superrun: int, run: int, logfile=None):
    """Starts the processes for the benchmark"""
    processes = []
    barrier = Barrier(config_dict["concurrentClients"])

    if logfile is not None:
        # Prepare a subfolder for this superrun and run
        logfile = f"{logfile}/logs_{config_dict['experimentName']}/run_{superrun}_{run}"
        # Create folder if not existing
        os.makedirs(os.path.dirname(logfile + "/"), exist_ok=True)

    queue = Queue()

    for i in range(config_dict["concurrentClients"]):
        processes.append(Process(target=run_benchmark,
                                    args=(config_dict, benchmark_param,
                                        barrier, queue, i, logfile)))
        processes[i].start()
    for i in range(config_dict["concurrentClients"]):
        processes[i].join()

    # First collect all results in a single result object for convenience.
    res = ClientResult()
    while not queue.empty():
        cr = queue.get()
        res.total_completed += cr.total_completed
        for il, num in cr.total_il_completed.items():
            if il not in res.total_il_completed:
                res.total_il_completed[il] = 0
            res.total_il_completed[il] += num
        res.num_deadlock_abort += cr.num_deadlock_abort
        res.num_conc_abort += cr.num_conc_abort
        res.num_serial_abort += cr.num_serial_abort
        for prgn, prgres in cr.programs.items():
            if prgn not in res.programs:
                res.programs[prgn] = ProgramResult(isolation_level=prgres.isolation_level)
            res_prgres = res.programs[prgn]
            res_prgres.total_completed += prgres.total_completed
            res_prgres.total_time += prgres.total_time
            res_prgres.num_deadlock_abort += prgres.num_deadlock_abort
            res_prgres.num_conc_abort += prgres.num_conc_abort
            res_prgres.num_serial_abort += prgres.num_serial_abort

    # Construct the expected result dict
    results = {}
    results["completedTotal"] = res.total_completed
    results["failed"] = {
        "deadlock" : res.num_deadlock_abort,
        "concurrentWrite" : res.num_conc_abort,
        "dangerousStructure" : res.num_serial_abort,
    }
    results["completed"] = {
        "RC" : res.total_il_completed.get("RC", 0),
        "SI" : res.total_il_completed.get("SI", 0),
        "SSI" : res.total_il_completed.get("SSI", 0)
    }
    results["programs"] = {}
    for program, prgres in res.programs.items():
        results["programs"][program] = {
            "failed" : {
                "deadlock" : prgres.num_deadlock_abort,
                "concurrentWrite" : prgres.num_conc_abort,
                "dangerousStructure" : prgres.num_serial_abort,
            },
            "completed" : {
                "RC" : (prgres.total_completed if prgres.isolation_level == "RC" else 0),
                "SI" : (prgres.total_completed if prgres.isolation_level == "SI" else 0),
                "SSI" : (prgres.total_completed if prgres.isolation_level == "SSI" else 0),
            },
            "runtime" : prgres.total_time
        }
    
    print(f"Completed total: {results['completedTotal']}")

    violations = benchmark_param.check_consistency(config_dict)
    if violations is not None:
        print(f"Number of violations: {violations}")
        results["violationRate"] = violations / results["completedTotal"]

    
    return results

def generate_pg_connection_url(config_dict: dict) -> str:
    """Generates a connection URL for the PostgreSQL database"""
    username = config_dict["dbUsername"]
    password = config_dict["dbPassword"]
    host = config_dict["dbUrl"]
    port = config_dict["dbPort"]
    database = config_dict["dbName"]
    return f"postgresql://{username}:{password}@{host}:{port}/{database}"

def run_benchmark(config_dict: dict, benchmark: Benchmark, barrier: mps.Barrier,
                  queue: Queue, process_id: int, logfile: Optional[str]) -> None:
    """Runs the benchmark"""
    connection = pg.connect(generate_pg_connection_url(config_dict))

    if logfile is not None:
        # The logfile is unique for each client process
        logfile = f"{logfile}/{process_id}.csv"

        with open(logfile, "w", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["Process_id", "Timestamp", "Program", "Account", "Event", "#RC error", "#SI error", "#SSI error", "Start", "End", "Duration"])

    # set session parameters
    cursor = connection.cursor()
    cursor.execute("SET enable_seqscan = OFF;")
    connection.commit()

    # Results object to collect results locally
    results = ClientResult()

    # Wait until all processes are ready
    barrier.wait()

    # Warmup phase: run transactions without recording results
    start_warmup = time.time()

    if logfile is not None:
        with open(logfile, "a", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow([process_id, start_warmup, "", "", "Info-Warmup", 0, 0, 0, 0, 0, 0, 0])

    while int(time.time() - start_warmup) < int(config_dict["timing"]["warmup"]):
        benchmark.run_transact(config_dict, connection, process_id, logfile)
    # print("Warmup done")

    # Experiment phase: run transactions and record results
    start_experiment = time.time()

    if logfile is not None:
        with open(logfile, "a", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow([process_id, start_experiment, "", "", "Info-Start", 0, 0, 0, 0, 0, 0, 0])

    while int(time.time() - start_experiment) < int(config_dict["timing"]["experiment"]):
        tres = benchmark.run_transact(config_dict, connection, process_id, logfile)

        # Store results locally
        results.total_completed += 1
        il = tres.isolation_level
        if il not in results.total_il_completed:
            results.total_il_completed[il] = 0
        results.total_il_completed[il] += 1
        results.num_deadlock_abort += tres.num_deadlock_abort
        results.num_conc_abort += tres.num_conc_abort
        results.num_serial_abort += tres.num_serial_abort
        if tres.program_name not in results.programs:
            results.programs[tres.program_name] = ProgramResult(isolation_level=il)
        prgres = results.programs[tres.program_name]
        prgres.total_completed += 1
        prgres.total_time += tres.total_time
        prgres.num_deadlock_abort += tres.num_deadlock_abort
        prgres.num_conc_abort += tres.num_conc_abort
        prgres.num_serial_abort += tres.num_serial_abort

    # Cooldown phase: run transactions without recording results
    start_cooldown = time.time()

    if logfile is not None:
        with open(logfile, "a", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow([process_id, start_cooldown, "", "", "Info-Cooldown", 0, 0, 0, 0, 0, 0, 0])

    while int(time.time() - start_cooldown) < int(config_dict["timing"]["extraTime"]):
        benchmark.run_transact(config_dict, connection, process_id, logfile)
    # print("Cooldown done")

    connection.close()

    # Store results in the queue
    queue.put(results)


def main():
    """The main function"""
    parser = argparse.ArgumentParser(description="Run the experiment defined in the config file")
    parser.add_argument("config")
    parser.add_argument("result")
    parser.add_argument("-log", "--log", help="Path to folder where log/ is made", required=False)
    args = parser.parse_args()

    config = read_config_file(args.config)

    # Allow overriding the DB URL with an environment variable
    db_url = os.getenv("DB_URL")
    if db_url is not None:
        config["dbUrl"] = db_url

    benchmark = get_benchmark(config["benchmark"])

    benchmark.check_config(config)
    results_superruns = {"superruns" : [],
                            "config" : config}
    for superrun in range(config["numberOfSuperruns"]):
        results_runs = {"runs" : []}
        for run in range(config["numberOfRuns"]):
            print(f"Superrun {superrun+1} of {config['numberOfSuperruns']}, run {run+1} of {config['numberOfRuns']}")
            benchmark.init_db(config)
            print("DB initialized!")
            # Wait a couple of seconds to avoid interference between database creation and the experiment
            #print("Waiting 2 seconds before starting the next run after creating the DB instance")
            time.sleep(2)
            print("Starting processes")
            result = start_processes(config, benchmark, superrun, run, args.log)
            print("Processes done")
            results_runs["runs"].append(result)
        results_superruns["superruns"].append(results_runs)
    write_results(sys.argv[2], results_superruns)


if __name__ == '__main__':
    main()
