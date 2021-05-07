#!/bin/bash

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

echo " "
echo ">>>> Tworzenie tematow wyjsciowych"

/usr/lib/kafka/bin/kafka-topics.sh --create --zookeeper ${CLUSTER_NAME}-m:2181 --replication-factor 1 --partitions 1 --topic anomaly-stocks

/usr/lib/kafka/bin/kafka-topics.sh --create --zookeeper ${CLUSTER_NAME}-m:2181 --replication-factor 1 --partitions 1 --topic aggregated



echo " "
echo ">>>> Pobieranie Elasticsearcha"

sudo docker pull elasticsearch:2.4.6
sudo docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" -e ES_JAVA_OPTS="-Xms750m -Xmx750m" elasticsearch:2.4.6 &


echo " "
echo ">>>> Pobieranie Elasticsearch Connectora"

git clone -b 0.10.0.0 https://github.com/confluentinc/kafka-connect-elasticsearch.git
wget https://ftp.man.poznan.pl/apache/maven/maven-3/3.8.1/binaries/apache-maven-3.8.1-bin.tar.gz
tar xzf apache-maven-3.8.1-bin.tar.gz

cd kafka-connect-elasticsearch
../apache-maven-3.8.1/bin/mvn clean package

sudo cp target/kafka-connect-elasticsearch-3.2.0-SNAPSHOT-package/share/java/kafka-connect-elasticsearch/* /usr/lib/kafka/libs
sudo rm /usr/lib/kafka/libs/slf4j-simple-1.7.5.jar
cd ..

echo " "
echo ">>>> Konfiguracja Connectora"
sudo cp /usr/lib/kafka/config/tools-log4j.properties /usr/lib/kafka/config/connect-log4j.properties

touch connector.properties
touch connect-standalone.properties
cat <<EOT >> connector.properties
name=elasticsearch-sink
connector.class=io.confluent.connect.elasticsearch.ElasticsearchSinkConnector
connection.url=http://localhost:9200
tasks.max=1
topics=aggregated,anomaly-stocks
type.name=stock
schema.ignore=true
EOT

cat <<EOT >> connect-standalone.properties
bootstrap.servers=${CLUSTER_NAME}-w-0:9092
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
internal.key.converter=org.apache.kafka.connect.storage.StringConverter
internal.value.converter=org.apache.kafka.connect.json.JsonConverter
internal.key.converter.schemas.enable=false
internal.value.converter.schemas.enable=false
offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=10000
EOT



echo " "
echo ">>>> Uruchomienie Connectora"
/usr/lib/kafka/bin/connect-standalone.sh connect-standalone.properties connector.properties &

echo " "
echo ">>>> Odczytywanie danych z Elasticsearcha"
while true; do curl -XGET '127.0.0.1:9200/aggregated/stock/_search?size=5&pretty' -H 'Content-Type: application/json' -d '{"sort" : [{ "year" : "desc" }, { "month" : "desc" }]}'; curl -XGET '127.0.0.1:9200/anomaly-stocks/stock/_search?size=5&pretty' -H 'Content-Type: application/json' -d '
{
  "sort" : [{ "end_date" : "desc" }]
}'; sleep 30; done
