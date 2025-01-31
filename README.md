# code-template-allocations-artifacts
Artifacts of our paper "Using Read Promotion and Mixed Isolation Levels for Performant Yet Serializable Execution of Transaction Programs".

This repository contains two parts:
- The source code of the allocation algorithm and the robustness test, presented in the paper.
- The throughput experiments (code and data).

## Robustness Test and Allocation Algorithm

The source code of the algorithm is available in the folder `template_robustness`:
- The algorithm is implemented in
`template_robustness/algorithm.py`.
- The lowest robust allocations for different promotion choices over the SmallBank benchmark are analysed in `template_robustness/smallbank.py`. To run the analysis:

```bash
python template_robustness/smallbank.py
```

## Throughput experiments

The source code and experiment data for the throughput experiments are available in the folder `throughput_experiments`. The experiment relies on a python package `cctest_core` that must be installed first. This package defines the general Benchmark protocol and implements the SmallBank benchmark. To install the package, run:
```bash
cd throughput_experiments/Core
pip install .
```
To run throughput experiments, execute the script `throughput_experiments/measure_throughput/experiment.py`. Note that this script requires two arguments: te path to the configuration file, and the path to the output file. The configuration file specifies the parameters of the experiment, such as the database connection parameters, the number of concurrent clients, hotspot size and probability, ...
Example configurations are available in the data folder.

The data is available in `throughput_experiments/data/smallbank`. This folder contains:
- the configuration files used for the experiments,
- the corresponding output files, and
- `visualizations.ipynb`, a notebook to explore the data.
