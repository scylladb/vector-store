# Configure and Run a ScyllaDB Cluster with Vector Search

## Introduction
This collection of scripts helps configure and run a **vector-search cluster test** on AWS.
The main entry point is `test.py`, which works in stages to install, configure, run, and load the cluster.

There are four types of nodes: **scylla**, **vector**, **client**, and **monitor**.
The script uses two configuration files:
- A **parameter file** that holds general information about the nodes.
- A **cluster.yml** file that contains the IP addresses of the nodes.

The scripts use **SSH** to perform remote operations.

For more details, refer to:
[Vector Search Benchmark Setup Guide](https://scylladb.atlassian.net/wiki/spaces/RND/pages/118358415/Vector+Search+Benchmark+Setup+Guide)

The `run_cluster.py` script takes a minimal configuration file, creates a cluster, and generates a file that describes that cluster.

### How the Script Works
The script creates a set of files locally and uploads them, along with other installation files, to each node.

To simplify this process:
- The **common** subdirectory contains files that will be uploaded to *all* machines.
- Each **type** subdirectory contains files that will be uploaded only to nodes of that specific type.
- During execution, the script generates a **directory per machine** under the `generated` directory and places machine-specific files there.

You can include additional files in these directories, and the script will upload them automatically.

Each machine type has its own `install.sh` script (used to perform installation on the remote machine) and a `run.sh` script (used to start the application).

## Prerequisites
- Create all the nodes in AWS. (We may add an option to automatically create the instances in the future.)
- Have an SSH key file that allows access to the machines.
- Fill the `cluster.yml` file with the IPs of the instances you created.
  The file is split into Availability Zones (AZs).
  You can place the monitor node anywhere you like, including as an additional entry without an AZ.

## Usage

### Create the required files and upload them
```bash
./test.py --key ~/.ssh/AmnonEc2.pem install
```

### Run the installation on each machine
```bash
./test.py --key ~/.ssh/AmnonEc2.pem configure
```

### Run the cluster
```bash
./test.py --key ~/.ssh/AmnonEc2.pem run-cluster
```

### Helpful Flags
Use `-V` for **verbose mode**.
Use `--dry-run` to print the actions that would be performed, without actually executing them.

## Unopinionated Definitions
In general, the scripts are designed to remain **unopinionated** about the node types.
The logic for configuring or running a specific node resides in its respective directory, making it easy to add new node types in the future if needed.
