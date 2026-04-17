package dev.irontools.flink.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Reads KafkaSource operator state (assigned splits) from a savepoint.
 *
 * <p>Each split is stored in ListState named "SourceReaderState" as byte[] elements
 * wrapped with SimpleVersionedSerialization: [version(4)][length(4)][splitData(N)].
 *
 * <p>The split data follows KafkaPartitionSplitSerializer format:
 * [topic(UTF)][partition(int)][startingOffset(long)][stoppingOffset(long)]
 *
 * <p>Run directly from IDE. Update the savepoint path and operator UID hash below before running.
 * Use {@link ReadSavepoint} to find the operator UID hash.
 *
 * <p>Requires JVM args: {@code --add-opens=java.base/java.lang=ALL-UNNAMED
 * --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED}
 */
public class ReadKafkaOffsets {

    private static final String SOURCE_READER_STATE_NAME = "SourceReaderState";
    private static final long NO_STOPPING_OFFSET = Long.MIN_VALUE;

    public static class KafkaPartitionSplitState {
        public String topic;
        public int partition;
        public long startingOffset;
        public long stoppingOffset;

        public KafkaPartitionSplitState() {}

        public KafkaPartitionSplitState(String topic, int partition, long startingOffset, long stoppingOffset) {
            this.topic = topic;
            this.partition = partition;
            this.startingOffset = startingOffset;
            this.stoppingOffset = stoppingOffset;
        }

        @Override
        public String toString() {
            String stoppingOffsetStr = stoppingOffset == NO_STOPPING_OFFSET ? "NONE" : String.valueOf(stoppingOffset);
            return "KafkaPartitionSplitState{topic='" + topic
                + "', partition=" + partition
                + ", startingOffset=" + startingOffset
                + ", stoppingOffset=" + stoppingOffsetStr + "}";
        }
    }

    static KafkaPartitionSplitState deserializeSplit(byte[] raw) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw))) {
            // SimpleVersionedSerialization wrapper
            int serializerVersion = in.readInt();
            int dataLength = in.readInt();

            // KafkaPartitionSplitSerializer format (version 0)
            String topic = in.readUTF();
            int partition = in.readInt();
            long startingOffset = in.readLong();
            long stoppingOffset = in.readLong();

            return new KafkaPartitionSplitState(topic, partition, startingOffset, stoppingOffset);
        }
    }

    public static void main(String[] args) throws Exception {
        String savepointPath = "/tmp/flink-savepoints/savepoint-e8a2a5-b8064464b991";
        String operatorUidHash = "cbc357ccb763df2852fee8c4fc7d55f2";

        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        SavepointReader savepoint = SavepointReader.read(
            env,
            savepointPath,
            new HashMapStateBackend()
        );

        savepoint.readListState(
                OperatorIdentifier.forUidHash(operatorUidHash),
                SOURCE_READER_STATE_NAME,
                TypeInformation.of(byte[].class)
            )
            .map(ReadKafkaOffsets::deserializeSplit)
            .print();

        env.execute("Read Kafka Offsets");
    }
}
