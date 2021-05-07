#!/bin/bash

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

echo " "
echo ">>>> Tworzenie tematow wejsciowych"

/usr/lib/kafka/bin/kafka-topics.sh --create --zookeeper ${CLUSTER_NAME}-m:2181 --replication-factor 1 --partitions 1 --topic stocks-results
/usr/lib/kafka/bin/kafka-topics.sh --create --zookeeper ${CLUSTER_NAME}-m:2181 --replication-factor 1 --partitions 1 --topic symbols

echo " "
echo ">>>> Pobieranie danych statycznych z katalogu symbols_valid_meta"

java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar com.example.bigdata.TestProducer symbols_valid_meta 0 symbols 1 ${CLUSTER_NAME}-w-0:9092

echo " "
echo ">>>> Pobieranie danych strumieniowych z katalogu stocks_result"

java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar com.example.bigdata.TestProducer stocks_result 10 stocks-results 1 ${CLUSTER_NAME}-w-0:9092
