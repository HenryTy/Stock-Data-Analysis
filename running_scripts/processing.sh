#!/bin/bash

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

java -cp /usr/lib/kafka/libs/*:stock-data.jar com.example.bigdata.StockDataProcessing ${CLUSTER_NAME}-w-0:9092 3 40
