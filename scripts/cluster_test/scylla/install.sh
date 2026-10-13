#!/bin/bash
. env.sh
sudo ~/install-python.sh
sudo ~/installer.py docker
sudo ~/installer.py node-exporter
