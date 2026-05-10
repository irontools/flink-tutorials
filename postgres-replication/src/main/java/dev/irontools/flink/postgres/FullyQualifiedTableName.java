package dev.irontools.flink.postgres;

import java.io.Serializable;

public record FullyQualifiedTableName(String schema, String table) implements Serializable {
  @Override
  public String toString() {
    return schema + "." + table;
  }
}
