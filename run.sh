#!/bin/sh
if [ "$#" -ne 1 ]; then
	echo "illegal number of parameters. Enter bash 2020201063.sh \"query;\""
	exit
fi
python3 test.py "$1"
