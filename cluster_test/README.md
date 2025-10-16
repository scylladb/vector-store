# Configure and running ScyllaDB cluster with vector search
## Introduction
This is collection of scrips is here to help configure vector-search cluster test on AWS.
The main entry point is test.py, it works in stages to install, configure, run and run load.

There are four types of nodes: "scylla", "vector", "client", "monitor".
The script uses two files for it's configuration a parameter file that holds general informatio about
the nodes and a cluster.yml file that contains the ip addresses of the nodes.

The scripts uses ssh to install to perform the operations.

### How the script works
The script create a banch of files locally and upload them with other installation files to each of the nodes.
To make it simpler there is a common sub dirctory, anything there will be uploaded to all machine.
Per type directory, anything there will be uploaded to the specific node type.
And last, when the script runs it will generate a directory per machine under the generated directory.
It will place there machine specific files.

You can include more files in those directory and the script will upload them.

Each machine type has its own install.sh command, that runs on the remove machine and uses the file
to complete the installation, and a run.sh command that is used to run that application.

## Prerequisit
Create all the nodes in AWS (we may add an option to create the instances for you in the future)
Have an ssh key file that will be used to reach the machines.

Fill the cluster.yml file with the ips of the images you created. the cluster.yml is split into AZ.
You can put the monitor anywhere you like including as an additional entry without an az.

# Usage
## Create needed file and upload them
```
./test.py --key ~/.ssh/AmnonEc2.pem install
```

## run the install.sh on each machine
```
./test.py --key ~/.ssh/AmnonEc2.pem configure
```

## run the cluster
```
./test.py --key ~/.ssh/AmnonEc2.pem run-cluster
```

## helpful flags
Use ```-V``` for verbose mode, you can use ```--dry-run``` in which the script would prints what it's going to do without doing anything.
