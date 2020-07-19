#!/bin/bash

##############################################################################
# Description:
# This is a helper script to download the Apache spark distros on the agent
#
# Usage:
# ./download-spark.sh <destination_path>
#
# Sample usage:
# ./download-spark.sh /usr/bin

##############################################################################

set +e

#Uncomment if you want full tracing (for debugging purposes)
#set -o xtrace

# Path where packaged worker file (tgz) exists.
DESTINATION_PATH=$1

mkdir -p "$DESTINATION_PATH"
cd $DESTINATION_PATH

echo "Downloading Spark distros."

curl -k -L -o spark-2.3.0.tgz https://archive.apache.org/dist/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz && tar xzvf spark-2.3.0.tgz
curl -k -L -o spark-2.3.1.tgz https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz && tar xzvf spark-2.3.1.tgz
curl -k -L -o spark-2.3.2.tgz https://archive.apache.org/dist/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz && tar xzvf spark-2.3.2.tgz
curl -k -L -o spark-2.3.3.tgz https://archive.apache.org/dist/spark/spark-2.3.3/spark-2.3.3-bin-hadoop2.7.tgz && tar xzvf spark-2.3.3.tgz
curl -k -L -o spark-2.3.4.tgz https://archive.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-hadoop2.7.tgz && tar xzvf spark-2.3.4.tgz
curl -k -L -o spark-2.4.0.tgz https://archive.apache.org/dist/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz && tar xzvf spark-2.4.0.tgz
curl -k -L -o spark-2.4.1.tgz https://archive.apache.org/dist/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz && tar xzvf spark-2.4.1.tgz
curl -k -L -o spark-2.4.3.tgz https://archive.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz && tar xzvf spark-2.4.3.tgz
curl -k -L -o spark-2.4.4.tgz https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz && tar xzvf spark-2.4.4.tgz
curl -k -L -o spark-2.4.5.tgz https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz && tar xzvf spark-2.4.5.tgz
curl -k -L -o spark-2.4.6.tgz https://archive.apache.org/dist/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz && tar xzvf spark-2.4.6.tgz
