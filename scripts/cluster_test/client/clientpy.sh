#!/bin/bash
cd /home/ubuntu/VectorDBBench
python3 -m venv benchmark
source ./benchmark/bin/activate
pip3 install -e '.[test]'
pip3 install -e '.[pinecone]'
pip3 install vectordb-bench[scylladb]