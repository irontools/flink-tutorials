package dev.irontools.flink.postgres;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.connector.jdbc.postgres.database.catalog.PostgresCatalog;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;

import java.io.Serial;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Replicates an arbitrary set of Postgres tables into another Postgres database, using a single
 * Flink CDC source for all tables. Schemas are discovered at startup via Flink's
 * {@link PostgresCatalog}.
 *
 * <p>Configuration is read from environment variables (see {@link JobConfig#fromEnv()} for the
 * full list and defaults). The defaults match the bundled {@code docker-compose.yml}.
 */
public class PostgresReplicationExample {

  public static void main(String[] args) throws Exception {
    JobConfig config = JobConfig.fromEnv();

    Configuration flinkConfig = new Configuration();
    // This is needed in order to prevent using Kryo
    // Also nice perf improvement
    flinkConfig.set(PipelineOptions.OBJECT_REUSE, true);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
    env.enableCheckpointing(config.checkpointIntervalMs());
    env.setParallelism(config.parallelism());

    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    Map<FullyQualifiedTableName, CatalogBaseTable> catalogTables = discoverSchemas(config);
    DataStream<SourceRecord> raw = buildCdcSource(env, config);

    StatementSet stmtSet = tEnv.createStatementSet();
    for (FullyQualifiedTableName id : config.tables()) {
      registerTablePipeline(tEnv, stmtSet, config, id, catalogTables.get(id), raw);
    }
    stmtSet.execute();
  }

  private static Map<FullyQualifiedTableName, CatalogBaseTable> discoverSchemas(JobConfig config)
      throws Exception {
    PostgresCatalog catalog = new PostgresCatalog(
        Thread.currentThread().getContextClassLoader(),
        "source_pg",
        config.sourceDatabase(),
        config.sourceUser(),
        config.sourcePassword(),
        String.format("jdbc:postgresql://%s:%d/", config.sourceHost(), config.sourcePort()));
    catalog.open();

    Map<FullyQualifiedTableName, CatalogBaseTable> result = new LinkedHashMap<>();
    for (FullyQualifiedTableName id : config.tables()) {
      ObjectPath path = new ObjectPath(config.sourceDatabase(), id.schema() + "." + id.table());
      result.put(id, catalog.getTable(path));
    }
    return result;
  }

  private static DataStream<SourceRecord> buildCdcSource(
      StreamExecutionEnvironment env, JobConfig config) {

    String[] tableList = config.tables().stream()
        .map(FullyQualifiedTableName::toString)
        .toArray(String[]::new);
    String[] schemaList = config.tables().stream()
        .map(FullyQualifiedTableName::schema)
        .distinct()
        .toArray(String[]::new);

    JdbcIncrementalSource<SourceRecord> source =
        PostgresSourceBuilder.PostgresIncrementalSource.<SourceRecord>builder()
            .hostname(config.sourceHost())
            .port(config.sourcePort())
            .database(config.sourceDatabase())
            .schemaList(schemaList)
            .tableList(tableList)
            .username(config.sourceUser())
            .password(config.sourcePassword())
            .slotName(config.slotName())
            .decodingPluginName("pgoutput")
            .deserializer(new RawDebeziumDeserializationSchema())
            .build();

    return env.fromSource(source, WatermarkStrategy.noWatermarks(), "PostgresCDCSource");
  }

  private static void registerTablePipeline(
      StreamTableEnvironment tEnv,
      StatementSet stmtSet,
      JobConfig config,
      FullyQualifiedTableName id,
      CatalogBaseTable catalogTable,
      DataStream<SourceRecord> raw) {

    Schema schema = catalogTable.getUnresolvedSchema();
    DataType dataType = Utils.extractRowDataTypeFromSchema(schema, tEnv);
    RowType rowType = (RowType) dataType.getLogicalType();

    String targetJdbcUrl = String.format("jdbc:postgresql://%s:%d/%s",
        config.targetHost(), config.targetPort(), config.targetDatabase());
    PgTableCreator.createIfNotExists(
        targetJdbcUrl, config.targetUser(), config.targetPassword(), id, schema, rowType);

    DebeziumDeserializationSchema<RowData> deserializer = RowDataDebeziumDeserializeSchema.newBuilder()
        .setPhysicalRowType(rowType)
        .setResultTypeInfo(InternalTypeInfo.of(rowType))
        .build();

    RowRowConverter rowConverter = RowRowConverter.create(dataType);

    @SuppressWarnings("unchecked")
    TypeInformation<Row> rowTypeInfo =
        (TypeInformation<Row>) TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(dataType);

    DataStream<Row> rows = raw
        .flatMap(new TableFilter(id))
        .returns(TypeInformation.of(SourceRecord.class))
        .name("Filter[" + id + "]")
        .flatMap(new SourceRecordToRow(deserializer, rowConverter))
        .returns(rowTypeInfo)
        .name("Deserialize[" + id + "]");

    Table sourceTable = tEnv.fromChangelogStream(rows, schema);

    TableDescriptor sinkDescriptor = TableDescriptor.forConnector("jdbc")
        .schema(schema)
        .option("url", targetJdbcUrl)
        .option("table-name", id.schema() + "." + id.table())
        .option("username", config.targetUser())
        .option("password", config.targetPassword())
        .option("sink.parallelism", String.valueOf(config.sinkParallelism()))
        .build();

    stmtSet.add(sourceTable.insertInto(sinkDescriptor));
  }

  /** Keeps only records originating from a specific {@link FullyQualifiedTableName}. */
  public static final class TableFilter implements FlatMapFunction<SourceRecord, SourceRecord> {
    @Serial
    private static final long serialVersionUID = 1L;
    private final FullyQualifiedTableName id;

    public TableFilter(FullyQualifiedTableName id) {
      this.id = id;
    }

    @Override
    public void flatMap(SourceRecord record, Collector<SourceRecord> out) {
      Struct envelope = (Struct) record.value();
      Struct source = envelope.getStruct("source");
      if (id.schema().equals(source.getString("schema"))
          && id.table().equals(source.getString("table"))) {
        out.collect(record);
      }
    }
  }

  /** Drives the per-table {@link RowDataDebeziumDeserializeSchema} and converts to {@link Row}. */
  public static final class SourceRecordToRow implements FlatMapFunction<SourceRecord, Row> {
    @Serial
    private static final long serialVersionUID = 1L;
    private final DebeziumDeserializationSchema<RowData> deserializer;
    private final RowRowConverter rowConverter;

    public SourceRecordToRow(
        DebeziumDeserializationSchema<RowData> deserializer,
        RowRowConverter rowConverter) {
      this.deserializer = deserializer;
      this.rowConverter = rowConverter;
    }

    @Override
    public void flatMap(SourceRecord record, Collector<Row> out) throws Exception {
      deserializer.deserialize(record, new Collector<>() {
        @Override
        public void collect(RowData rowData) {
          out.collect(rowConverter.toExternal(rowData));
        }

        @Override
        public void close() {}
      });
    }
  }
}
