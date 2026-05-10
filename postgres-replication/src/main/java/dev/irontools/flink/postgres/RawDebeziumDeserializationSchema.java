package dev.irontools.flink.postgres;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;

import java.io.Serial;

/** Pass-through deserializer that forwards Debezium {@link SourceRecord} unchanged. */
public class RawDebeziumDeserializationSchema implements DebeziumDeserializationSchema<SourceRecord> {
  @Serial
  private static final long serialVersionUID = 1L;

  @Override
  public void deserialize(SourceRecord record, Collector<SourceRecord> out) {
    out.collect(record);
  }

  @Override
  public TypeInformation<SourceRecord> getProducedType() {
    return TypeInformation.of(SourceRecord.class);
  }
}
