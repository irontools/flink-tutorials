package dev.irontools.flink.kafka;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Reads savepoint metadata using Flink's built-in state module.
 * Prints all operator names, UID hashes, and other metadata.
 *
 * <p>Run directly from IDE. Update the savepoint path below before running.
 */
public class ReadSavepoint {
  public static void main(String[] args) {
      String savepointPath = "/tmp/flink-savepoints/savepoint-e8a2a5-b8064464b991";

      EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
      settings.getConfiguration().setString("table.display.max-column-width", "36");
      TableEnvironment tEnv = TableEnvironment.create(settings);

      tEnv.executeSql("LOAD MODULE state");
      tEnv.executeSql("SELECT * FROM savepoint_metadata('" + savepointPath + "')").print();
    }
}
