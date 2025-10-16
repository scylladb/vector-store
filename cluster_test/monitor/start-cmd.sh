#!/bin/bash
./generate-dashboards.sh -F -v 2025.4
./start-all.sh -v 2025.4 --vector-search vector_search_servers.yml --target-directory targets -d prometheus_data
