if [ "$1" == "" ] || [ "$2" == "" ] || [ "$3" == "" ]; then
	echo "Usage path/to/keys ip source/file"
	exit 0
fi
KEYS=$1
IP=$2
FROM=$3
scp -i $KEYS $FROM ubuntu@$IP:~/
