#!/bin/bash
. env.sh
sudo ~/install-python.sh
sudo ~/installer.py node-exporter
sudo ~/installer.py vector-search --version $VERSION
