package dev.irontools.flink.kafka;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.SavepointWriter;
import org.apache.flink.state.api.StateBootstrapTransformation;
import org.apache.flink.state.api.functions.StateBootstrapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Writes KafkaSource operator state (partition split assignments) into a new or existing savepoint.
 * Also writes the enumerator coordinator state so that the KafkaSourceEnumerator knows which
 * partitions are already assigned and does not re-assign them from earliest offset.
 *
 * <p>Build the shaded JAR first: {@code mvn package}
 *
 * <p>Run directly from IDE. Update the paths, operator UID hash, and split definitions
 * in {@code main()} before running. Use {@link ReadSavepoint} to find the operator UID hash.
 *
 * <p>Requires JVM args: {@code --add-opens=java.base/java.lang=ALL-UNNAMED
 * --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED}
 */
public class WriteKafkaOffsets {

    private static final String SOURCE_READER_STATE_NAME = "SourceReaderState";
    private static final long NO_STOPPING_OFFSET = Long.MIN_VALUE;
    private static final int KAFKA_SPLIT_SERIALIZER_VERSION = 0;
    private static final int KAFKA_ENUM_STATE_SERIALIZER_VERSION = 2;
    private static final int SOURCE_COORDINATOR_SERDE_VERSION = 1;
    private static final int ASSIGNMENT_STATUS_ASSIGNED = 0;

    static class SplitInfo {
        final String topic;
        final int partition;
        final long offset;

        SplitInfo(String topic, int partition, long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }
    }

    static byte[] serializeSplit(SplitInfo split) throws IOException {
        // KafkaPartitionSplitSerializer data
        ByteArrayOutputStream splitBaos = new ByteArrayOutputStream();
        DataOutputStream splitOut = new DataOutputStream(splitBaos);
        splitOut.writeUTF(split.topic);
        splitOut.writeInt(split.partition);
        splitOut.writeLong(split.offset);
        splitOut.writeLong(NO_STOPPING_OFFSET);
        splitOut.flush();
        byte[] splitData = splitBaos.toByteArray();

        // SimpleVersionedSerialization wrapper: [version(4)][length(4)][data]
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeInt(KAFKA_SPLIT_SERIALIZER_VERSION);
        out.writeInt(splitData.length);
        out.write(splitData);
        out.flush();
        return baos.toByteArray();
    }

    /**
     * Serializes the KafkaSourceEnumerator coordinator state.
     *
     * <p>The full format has three layers:
     * <ol>
     *   <li>SourceCoordinator serde version header (int) — currently version 1
     *   <li>SimpleVersionedSerialization wrapper: [enumSerializerVersion(int)][dataLength(int)][data]
     *   <li>KafkaSourceEnumStateSerializer v2 data: partitions with assignment status + discovery flag
     * </ol>
     */
    static byte[] serializeEnumeratorState(List<SplitInfo> splits) throws IOException {
        // Layer 3: KafkaSourceEnumStateSerializer v2 format
        ByteArrayOutputStream enumBaos = new ByteArrayOutputStream();
        DataOutputStream enumOut = new DataOutputStream(enumBaos);
        enumOut.writeInt(splits.size());
        for (SplitInfo split : splits) {
            enumOut.writeUTF(split.topic);
            enumOut.writeInt(split.partition);
            enumOut.writeInt(ASSIGNMENT_STATUS_ASSIGNED);
        }
        enumOut.writeBoolean(true); // initialDiscoveryFinished
        enumOut.flush();
        byte[] enumData = enumBaos.toByteArray();

        // Layer 1 + 2: coordinator serde version + SimpleVersionedSerialization wrapper
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeInt(SOURCE_COORDINATOR_SERDE_VERSION);
        out.writeInt(KAFKA_ENUM_STATE_SERIALIZER_VERSION);
        out.writeInt(enumData.length);
        out.write(enumData);
        out.flush();
        return baos.toByteArray();
    }

    static OperatorID operatorIdFromHex(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return new OperatorID(bytes);
    }

    /**
     * Post-processes the savepoint _metadata file to inject coordinator state for the
     * KafkaSource enumerator. The _metadata file is always small (a few KB) regardless
     * of savepoint size — it only contains metadata pointers, not actual state data.
     */
    static void injectCoordinatorState(String savepointPath, String operatorUidHash, byte[] coordinatorState) throws Exception {
        Path metadataPath = Paths.get(savepointPath, "_metadata");

        CheckpointMetadata metadata;
        try (DataInputStream dis = new DataInputStream(Files.newInputStream(metadataPath))) {
            metadata = Checkpoints.loadCheckpointMetadata(
                dis, Thread.currentThread().getContextClassLoader(), savepointPath);
        }

        OperatorID targetId = operatorIdFromHex(operatorUidHash);
        boolean found = false;
        for (OperatorState opState : metadata.getOperatorStates()) {
            if (opState.getOperatorID().equals(targetId)) {
                opState.setCoordinatorState(
                    new ByteStreamStateHandle("kafka-enumerator", coordinatorState));
                found = true;
                break;
            }
        }

        if (!found) {
            throw new IllegalStateException(
                "Operator " + operatorUidHash + " not found in savepoint metadata");
        }

        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(metadataPath))) {
            Checkpoints.storeCheckpointMetadata(metadata, dos);
        }
    }

    public static class KafkaSourceStateBootstrapFunction extends StateBootstrapFunction<byte[]> {

        private ListState<byte[]> sourceReaderState;

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            sourceReaderState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>(SOURCE_READER_STATE_NAME, byte[].class)
            );
        }

        @Override
        public void processElement(byte[] value, Context ctx) throws Exception {
            sourceReaderState.add(value);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
        }
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "/tmp/flink-savepoints/savepoint-e8a2a5-b8064464b991";
        String outputPath = "/tmp/flink-savepoints/generated-savepoint";
        String operatorUidHash = "cbc357ccb763df2852fee8c4fc7d55f2";

        // TODO: modify topic/partitions/offsets
        List<SplitInfo> splits = Arrays.asList(
            new SplitInfo("loadtest.json", 0, 0),
            new SplitInfo("loadtest.json", 1, 0),
            new SplitInfo("loadtest.json", 2, 0)
        );

        Configuration conf = new Configuration();
        conf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        List<byte[]> serializedSplits = splits.stream()
            .map(s -> { try { return serializeSplit(s); } catch (IOException e) { throw new RuntimeException(e); } })
            .toList();

        DataStream<byte[]> splitStream = env.fromCollection(serializedSplits);

        StateBootstrapTransformation<byte[]> transformation = OperatorTransformation
            .bootstrapWith(splitStream)
            .transform(new KafkaSourceStateBootstrapFunction());

        OperatorIdentifier operatorId = OperatorIdentifier.forUidHash(operatorUidHash);

        SavepointWriter
            .fromExistingSavepoint(env, inputPath, new HashMapStateBackend())
            .removeOperator(operatorId)
            .withOperator(operatorId, transformation)
            .write(outputPath);

        env.execute("Write Kafka Offsets");

        byte[] enumeratorState = serializeEnumeratorState(splits);
        injectCoordinatorState(outputPath, operatorUidHash, enumeratorState);
    }
}
