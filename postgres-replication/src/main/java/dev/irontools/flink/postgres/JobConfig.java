package dev.irontools.flink.postgres;

import java.util.Arrays;
import java.util.List;

/** Job configuration loaded from environment variables. */
public record JobConfig(
    String sourceHost,
    int sourcePort,
    String sourceDatabase,
    String sourceUser,
    String sourcePassword,
    String targetHost,
    int targetPort,
    String targetDatabase,
    String targetUser,
    String targetPassword,
    List<FullyQualifiedTableName> tables,
    String slotName,
    int parallelism,
    int sinkParallelism,
    long checkpointIntervalMs) {

  public static JobConfig fromEnv() {
    return new JobConfig(
        env("SOURCE_HOST", "localhost"),
        envInt("SOURCE_PORT", 35432),
        env("SOURCE_DATABASE", "source"),
        env("SOURCE_USER", "flink"),
        env("SOURCE_PASSWORD", "flink"),
        env("TARGET_HOST", "localhost"),
        envInt("TARGET_PORT", 35433),
        env("TARGET_DATABASE", "target"),
        env("TARGET_USER", "flink"),
        env("TARGET_PASSWORD", "flink"),
        parseTables(env("TABLES", "public.users,public.orders")),
        env("SLOT_NAME", "flink_cdc_replication"),
        envInt("PARALLELISM", 8),
        envInt("SINK_PARALLELISM", 8),
        envLong("CHECKPOINT_INTERVAL_MS", 10_000L));
  }

  private static String env(String name, String fallback) {
    String value = System.getenv(name);
    return (value == null || value.isBlank()) ? fallback : value;
  }

  private static int envInt(String name, int fallback) {
    String value = System.getenv(name);
    return (value == null || value.isBlank()) ? fallback : Integer.parseInt(value.trim());
  }

  private static long envLong(String name, long fallback) {
    String value = System.getenv(name);
    return (value == null || value.isBlank()) ? fallback : Long.parseLong(value.trim());
  }

  private static List<FullyQualifiedTableName> parseTables(String spec) {
    return Arrays.stream(spec.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(JobConfig::parseTable)
        .toList();
  }

  private static FullyQualifiedTableName parseTable(String spec) {
    int dot = spec.indexOf('.');
    if (dot <= 0 || dot == spec.length() - 1) {
      throw new IllegalArgumentException(
          "Invalid table identifier (expected schema.table): " + spec);
    }
    return new FullyQualifiedTableName(spec.substring(0, dot), spec.substring(dot + 1));
  }
}
