Overview

This service maintains the latest price per financial instrument based on asOf timestamp.
Producers publish price data in batch runs, and consumers can query the last successful price for an instrument.

The system is event-driven, in-memory, and resilient to incorrect producer order or concurrent consumer access.

Architecture
Client (REST)
   |
   v
Price Batch Controller
   |
   v
Kafka Producer
   |
   v
Kafka Topic (price-batch-topic)
   |
   v
Kafka Consumer
   |
   v
InMemoryPriceStore
   |
   v
Read API (Last Price)

Why Kafka?

Decouples producers from consumers

Ensures atomic visibility of batch completion

Supports async ingestion without explicit thread management

Scales horizontally if needed later

Core Concepts
Price Record
{
  "id": "INSTRUMENT_1",
  "asOf": "2026-01-11T08:30:00Z",
  "payload": {
    "price": 123.45,
    "currency": "USD"
  }
}


id: Instrument identifier

asOf: Determines latest price

payload: Flexible structure

Batch Lifecycle

Start Batch

Upload Chunks (≤ 1000 records per chunk)

Complete Batch → prices become visible atomically
OR

Cancel Batch → data discarded

Cancelled or incomplete batches are never exposed to consumers.

Kafka Design
Topic

Name: price-batch-topic

Partitions: 1 (in-memory, ordering guaranteed)

Key: batchId

Value: PriceBatchMessage

Message Types

START

DATA

COMPLETE

CANCEL

Serialization

Producer: JsonSerializer

Consumer: JsonDeserializer

Jackson JavaTimeModule enabled for Instant

REST APIs
Start Batch
POST /api/batch/start


Response

{ "batchId": "uuid" }

Upload Batch Chunk
POST /api/batch/{batchId}/data

Complete Batch
POST /api/batch/{batchId}/complete

Cancel Batch
POST /api/batch/{batchId}/cancel

Get Last Price
GET /api/prices/{instrumentId}

Concurrency & Safety

Consumers can safely read while batches are processing

Latest price determined strictly by asOf

Batch completion is atomic

Thread safety achieved via:

Concurrent maps

Kafka’s single-threaded partition consumption

👉 No manual threads required — Kafka handles async processing.

Testing
Unit Tests

Batch completion success

Batch cancellation

Last price selection by asOf

Invalid order handling

Integration Tests

Kafka producer → consumer flow

End-to-end REST + Kafka validation

Kafka is tested using embedded Kafka.

Configuration (application.properties)
spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer

spring.kafka.consumer.group-id=price-service-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.trusted.packages=*
spring.kafka.consumer.properties.spring.json.use.type.headers=false
spring.kafka.consumer.properties.spring.json.value.default.type=com.SP.LastPriceJava.kafka.PriceBatchMessage

Tech Stack

Java 21

Spring Boot

Spring Kafka

Jackson (JSR-310)

JUnit 5

Embedded Kafka
