#!/bin/bash
sudo apt-get update --fix-missing -y > /dev/null 2>&1
sudo apt-get upgrade -y > /dev/null 2>&1
sudo apt-get install -y python3-setuptools > /dev/null 2>&1
sudo apt-get install -y python3-pip python3-venv > /dev/null 2>&1
