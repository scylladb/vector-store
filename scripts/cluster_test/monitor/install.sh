#!/bin/bash
. env.sh
sudo ./installer.py docker
sudo ./installer.py node-exporter
sudo ./installer.py monitor --version $VERSION
mkdir -p /home/ubuntu/scylla-monitoring/targets
cp vector_servers.yml /home/ubuntu/scylla-monitoring/targets/vector_search_servers.yml
cp scylla_servers.yml /home/ubuntu/scylla-monitoring/targets/scylla_servers.yml
cp scylla_servers.yml /home/ubuntu/scylla-monitoring/targets/node_exporter_servers.yml
cp ./start-cmd.sh /home/ubuntu/scylla-monitoring/
