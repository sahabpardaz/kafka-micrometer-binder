# Kafka Micrometer Binder
[![Tests](https://github.com/sahabpardaz/kafka-micrometer-binder/actions/workflows/maven.yml/badge.svg?branch=master)](https://github.com/sahabpardaz/kafka-micrometer-binder/actions/workflows/maven.yml)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=sahabpardaz_kafka-micrometer-binder&metric=coverage)](https://sonarcloud.io/dashboard?id=sahabpardaz_kafka-micrometer-binder)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=sahabpardaz_kafka-micrometer-binder&metric=alert_status)](https://sonarcloud.io/dashboard?id=sahabpardaz_kafka-micrometer-binder)

An implementation of Kafka `MetricsReporter` which binds Kafka client metrics to Micrometer.

Kafka has a built-in mechanism which allows us to gather its metrics and report them to different sources.
In order to do so, we should implement `MetricsReporter` interface and register our class in client configs.
After that, our implementation will receive metrics events from Kafka. The purpose of this library is to gather
Kafka metrics and reports them to micrometer registry.

## Usage
In Kafka consumer/producer configs you should set `metric.reporters` property to `ir.sahab.micrometer.kafka.MicrometerKafkaMetricsReporter`.
Also, you should set a unique `client.id` which helps distinguish different clients' metrics in the same JVM.

for example:
```java
KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
        "client.id", "events-consumer",
        "metric.reporters", MicrometerKafkaMetricsReporter.class.getName(),
        ...
        ));
```
Reported metrics will be like:
```
kafka.consumer.node.outgoing.byte.rate{client-id="events-consumer", node-id="node--1"}
kafka.consumer.coordinator.rebalance.total{client-id="events-consumer"}
...
```
