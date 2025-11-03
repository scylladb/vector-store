#!/usr/bin/env bash
if [ "$1" == "" ] || [ "$2" == "" ] || [ "$3" == "" ]; then
	echo "Usage path/to/keys ip cmd"
	exit 0
fi
KEYS=$1
IP=$2
CMD=$3
ssh -i $KEYS -o StrictHostKeyChecking=no ubuntu@$IP $CMD
