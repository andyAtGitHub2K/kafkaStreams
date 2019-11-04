#!/bin/bash

KAFKA_BIN="/home/babee/Downloads/kafka_2.12-1.0.0/bin"

declare -a arr=("chapter4-zmrt-in-topic"
                "chapter4-pattern"
                "chapter4-zmrt-out-topic"
                "beer-out"
                "book-out"
                "customer-trans"
                "chapter4-reward"
                )

# start zookeeper
# ./bin/zookeeper-server-start.sh ./config/zookeeper.properties
# start kafka
# ./bin/kafka-server-start.sh ./config/server.properties


# create topics
for item in "${arr[@]}"
do
  $KAFKA_BIN/kafka-topics.sh --create  --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $item
done
#$KAFKA_BIN/kafka-topics.sh --create  --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic "customer_transactions_topic"
# list topics

$KAFKA_BIN/kafka-topics.sh --list --zookeeper localhost:2181

# consume topic
#kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic customer_transactions_topic --from-beginning

# delete
# ./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic 'giorgos-.*'