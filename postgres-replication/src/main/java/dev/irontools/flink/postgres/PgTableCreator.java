package dev.irontools.flink.postgres;

import org.apache.flink.connector.jdbc.postgres.database.catalog.PostgresTypeMapper;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility for creating Postgres schemas and tables from a Flink {@link Schema} / {@link RowType}.
 *
 * <p>The Flink → Postgres type mapping in {@link #getMapping(LogicalType)} is the inverse of
 * {@link PostgresTypeMapper#mapping}: every Postgres type root the catalog mapper recognises
 * round-trips here, including arrays.
 */
public final class PgTableCreator {
  private static final Logger LOG = LoggerFactory.getLogger(PgTableCreator.class);

  private PgTableCreator() {}

  /**
   * Connects to {@code jdbcUrl}, runs {@code CREATE SCHEMA IF NOT EXISTS} for {@code id.schema()},
   * then {@code CREATE TABLE IF NOT EXISTS} for {@code id} using DDL derived from {@code schema}
   * and {@code rowType} (column types from the row type, primary key from the schema).
   */
  public static void createIfNotExists(
      String jdbcUrl,
      String username,
      String password,
      FullyQualifiedTableName id,
      Schema schema,
      RowType rowType) {
    try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
         Statement stmt = conn.createStatement()) {
      String createSchema = buildCreateSchemaDdl(id.schema());
      LOG.info("Ensuring Postgres schema: {}", createSchema);
      stmt.execute(createSchema);

      String createTable = buildCreateTableDdl(id, schema, rowType);
      LOG.info("Ensuring Postgres table: {}", createTable);
      stmt.execute(createTable);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to create Postgres table " + id, e);
    }
  }

  public static String buildCreateSchemaDdl(String schemaName) {
    return String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", schemaName);
  }

  public static String buildCreateTableDdl(
      FullyQualifiedTableName id, Schema schema, RowType rowType) {
    List<String> columnDefs = new ArrayList<>();
    for (RowType.RowField field : rowType.getFields()) {
      String pgType = getMapping(field.getType());
      String nullable = field.getType().isNullable() ? "" : " NOT NULL";
      columnDefs.add("\"" + field.getName() + "\" " + pgType + nullable);
    }

    schema.getPrimaryKey().ifPresent(pk -> {
      String pkCols = pk.getColumnNames().stream()
          .map(name -> "\"" + name + "\"")
          .collect(Collectors.joining(", "));
      columnDefs.add("PRIMARY KEY (" + pkCols + ")");
    });

    return String.format(
        "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (%s)",
        id.schema(), id.table(), String.join(", ", columnDefs));
  }

  /**
   * Inverse of {@link PostgresTypeMapper#mapping}. Returns the Postgres DDL fragment for a Flink
   * {@link LogicalType}. Covers every type root that the catalog type mapper produces.
   */
  public static String getMapping(LogicalType type) {
    return switch (type.getTypeRoot()) {
      case BOOLEAN -> "BOOLEAN";
      case BINARY, VARBINARY -> "BYTEA";
      case TINYINT, SMALLINT -> "SMALLINT";
      case INTEGER -> "INTEGER";
      case BIGINT -> "BIGINT";
      case FLOAT -> "REAL";
      case DOUBLE -> "DOUBLE PRECISION";
      case DECIMAL -> {
        DecimalType d = (DecimalType) type;
        yield String.format("NUMERIC(%d, %d)", d.getPrecision(), d.getScale());
      }
      case CHAR -> {
        CharType c = (CharType) type;
        yield String.format("CHAR(%d)", c.getLength());
      }
      case VARCHAR -> {
        VarCharType v = (VarCharType) type;
        // Flink STRING() == VARCHAR(MAX_LENGTH); Postgres has no fixed cap, use TEXT.
        yield v.getLength() == VarCharType.MAX_LENGTH
            ? "TEXT"
            : String.format("VARCHAR(%d)", v.getLength());
      }
      case DATE -> "DATE";
      case TIME_WITHOUT_TIME_ZONE -> {
        TimeType t = (TimeType) type;
        yield String.format("TIME(%d)", t.getPrecision());
      }
      case TIMESTAMP_WITHOUT_TIME_ZONE -> {
        TimestampType t = (TimestampType) type;
        yield String.format("TIMESTAMP(%d)", t.getPrecision());
      }
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> {
        LocalZonedTimestampType t = (LocalZonedTimestampType) type;
        yield String.format("TIMESTAMP(%d) WITH TIME ZONE", t.getPrecision());
      }
      case TIMESTAMP_WITH_TIME_ZONE -> {
        ZonedTimestampType t = (ZonedTimestampType) type;
        yield String.format("TIMESTAMP(%d) WITH TIME ZONE", t.getPrecision());
      }
      case ARRAY -> {
        ArrayType arr = (ArrayType) type;
        yield getMapping(arr.getElementType()) + "[]";
      }
      default -> {
        LOG.warn("No Postgres DDL mapping for Flink type {}; falling back to TEXT", type);
        yield "TEXT";
      }
    };
  }
}
