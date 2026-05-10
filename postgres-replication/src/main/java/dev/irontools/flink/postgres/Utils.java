package dev.irontools.flink.postgres;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.DataType;

public final class Utils {
  private Utils() {}

  /**
   * Resolves an unresolved Flink {@link Schema} (e.g. one returned by a JDBC catalog) into a
   * concrete row {@link DataType} usable by RowType-based deserializers.
   */
  public static DataType extractRowDataTypeFromSchema(Schema schema, StreamTableEnvironment tEnv) {
    DataTypeFactory factory =
        ((TableEnvironmentInternal) tEnv).getCatalogManager().getDataTypeFactory();

    DataTypes.AbstractField[] fields = schema.getColumns().stream()
        .map(column -> {
          Schema.UnresolvedPhysicalColumn physical = (Schema.UnresolvedPhysicalColumn) column;
          return DataTypes.FIELD(physical.getName(), physical.getDataType());
        })
        .toArray(DataTypes.AbstractField[]::new);

    return DataTypes.ROW(fields).toDataType(factory);
  }
}
