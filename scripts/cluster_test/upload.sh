#!/usr/bin/env bash
if [ "$1" == "" ] || [ "$2" == "" ] || [ "$3" == "" ]; then
	echo "Usage path/to/keys ip source/file"
	exit 0
fi
KEYS=$1
IP=$2
FROM=$3
echo "Uploading $FROM to $IP"
scp -i $KEYS -o StrictHostKeyChecking=no $FROM ubuntu@$IP:~/
