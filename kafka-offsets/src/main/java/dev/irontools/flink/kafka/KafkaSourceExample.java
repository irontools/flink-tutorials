package dev.irontools.flink.kafka;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Reads from a Kafka topic and prints to stdout via Flink Table API.
 *
 * <p>Taking a savepoint via the REST API. The REST port is random when running locally —
 * check the log output for the actual port (replace PORT and JOB_ID accordingly):
 * <pre>
 * curl -s -X POST http://localhost:PORT/jobs/JOB_ID/savepoints
 * curl -s http://localhost:PORT/jobs/JOB_ID/savepoints/TRIGGER_ID
 * </pre>
 *
 * <p>Stop with savepoint:
 * <pre>
 * curl -s -X POST -H 'Content-Type:application/json' -d '{"drain":false}' http://localhost:PORT/jobs/JOB_ID/stop
 * </pre>
 */
public class KafkaSourceExample {
  public static void main(String[] args) {
    EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    settings.getConfiguration().setString("execution.checkpointing.interval", "10s");
    settings.getConfiguration().setString("execution.checkpointing.savepoint-dir", "file:///tmp/flink-savepoints");
    // settings.getConfiguration().setString("execution.state-recovery.path", "/tmp/flink-savepoints/generated-savepoint");
    TableEnvironment tEnv = TableEnvironment.create(settings);

    tEnv.executeSql("""
      CREATE TABLE LoadTest (
        createdAt BIGINT,
        latitude DOUBLE,
        jobTitle STRING,
        randomIdentifier BIGINT,
        active BOOLEAN,
        id STRING
      ) WITH (
        'connector' = 'kafka',
        'topic' = 'loadtest.json',
        'properties.bootstrap.servers' = 'localhost:39092',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
      )
      """);

    tEnv.executeSql("""
      CREATE TABLE PrintSink (
        createdAt BIGINT,
        latitude DOUBLE,
        jobTitle STRING,
        randomIdentifier BIGINT,
        active BOOLEAN,
        id STRING
      ) WITH (
        'connector' = 'print'
      )
      """);

    tEnv.executeSql("""
      INSERT INTO PrintSink
      SELECT * FROM LoadTest
      """);
  }
}
