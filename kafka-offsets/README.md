# kafka-offsets

Read and modify Kafka consumer offsets in Flink savepoints using the State Processor API.

Uses:

- Flink 2.1.1
- flink-connector-kafka 4.0.1-2.0

## Blog post

Detailed walkthrough: [Reading and Modifying Kafka Consumer Offsets Using the State Processor API](https://streamacademy.io/tutorial/flink-kafka-consumer-offsets-with-the-state-processor-api/).

## Components

- **KafkaSourceExample** -- Flink Table API job that reads from a Kafka topic and prints to stdout. Includes checkpointing and savepoint recovery.
- **ReadSavepoint** -- Prints savepoint metadata (operator names, UID hashes) using Flink's `savepoint_metadata()` function.
- **ReadKafkaOffsets** -- Reads Kafka partition offsets from a savepoint's `SourceReaderState`.
- **WriteKafkaOffsets** -- Writes modified Kafka offsets into an existing savepoint, including both reader state and enumerator coordinator state.

## Infrastructure

```
docker compose up -d
```

Starts Redpanda (Kafka on port 39092), Redpanda Console (port 38080), and a Python data generator that produces 100 JSON messages to `loadtest.json` topic.

## Usage

1. Run `KafkaSourceExample` to start consuming. Take a savepoint via the REST API.
2. Run `ReadSavepoint` to find the Kafka source operator UID hash.
3. Run `ReadKafkaOffsets` to inspect the current offsets.
4. Edit offsets in `WriteKafkaOffsets` and run it to produce a modified savepoint.
5. Restart `KafkaSourceExample` from the modified savepoint.

All utilities are designed to run from an IDE. Update the hardcoded paths and operator UID hash before running. `ReadKafkaOffsets` and `WriteKafkaOffsets` require `--add-opens` JVM args (see Javadocs).
