# postgres-replication

Replicate an arbitrary set of Postgres tables into another Postgres database using a single Apache Flink CDC source. Source schemas are discovered automatically. Target tables are also created automatically if needed.

Uses:

- Flink 2.2.0 
- flink-sql-connector-postgres-cdc 3.6.0-2.2 
- flink-connector-jdbc-postgres 4.0.0-2.0

## Blog post

Detailed walkthrough: [Postgres to Postgres Replication with Flink CDC](https://streamacademy.io/tutorial/flink-postgres-cdc-replication/).

## Pipeline

```
                         ┌──────────────── per table ────────────────┐
PostgresCDC source       │ flatMap(filter)     flatMap(deserialize)  │
   (raw SourceRecord) ──>│  SourceRecord  ──>  RowData  ──>  Row     │──> fromChangelogStream ──> JDBC sink
                         └───────────────────────────────────────────┘
```

- One CDC source serves every table — one replication slot, one snapshot pass.
- The deserializer is a pass-through so `SourceRecord` flows through the operator graph unchanged.
- A demux flat-map filters by the Debezium envelope's `source.schema` / `source.table`.
- Per table, a `RowDataDebeziumDeserializeSchema` built from that table's `RowType` (discovered via Flink's `PostgresCatalog`) converts to `RowData`.
- A `RowRowConverter` lifts `RowData` → `Row` only at the boundary into the Table API (`fromChangelogStream`).
- Each table writes through a JDBC `TableDescriptor` sink. A single `StatementSet` executes them all in one job.

## Infrastructure

```
docker compose up -d
```

Starts:
- **postgres-source** -- source Postgres on port `35432`, configured with `wal_level=logical` and pre-created `users` + `orders` tables (with `REPLICA IDENTITY FULL`).
- **postgres-target** -- target Postgres on port `35433`. **Empty** — no tables are pre-created. The Flink job calls `PgTableCreator` to run `CREATE SCHEMA IF NOT EXISTS` + `CREATE TABLE IF NOT EXISTS` on each target before its sink opens, with DDL derived from the corresponding source schema.
- **generator** -- Python script that emits 100 change events split between `users` and `orders`.

Both databases use credentials `flink` / `flink`.

## Usage

1. `docker compose up -d` to start both Postgres instances and the generator.
2. Run `PostgresReplicationExample` from your IDE. The job snapshots `users` + `orders`, then streams change events.
3. Verify the replicas:

   ```
   docker compose exec postgres-target psql -U flink -d target -c 'SELECT count(*) FROM users; SELECT count(*) FROM orders;'
   ```

4. Mutate the source manually and watch it propagate:

   ```
   docker compose exec postgres-source psql -U flink -d source -c \
     "UPDATE users SET active = NOT active WHERE id = (SELECT id FROM users LIMIT 1);"
   ```

## Why the SQL/uber CDC connector

Flink CDC `3.6.0-2.2` was compiled against `flink-shaded-guava 31.x` (`shaded.guava31.*`); Flink 2.2 itself ships `flink-shaded-guava 33.x` (`shaded.guava33.*`). Both shaded packages have non-overlapping class names, so they coexist at runtime — but Maven resolves only **one version per `groupId:artifactId`**, so you can't put both on the classpath via plain `<dependency>` declarations.

Using `flink-sql-connector-postgres-cdc:3.6.0-2.2` (the uber jar) sidesteps the conflict: it bundles `guava31` internally at `org.apache.flink.shaded.guava31.*`, leaving Maven free to resolve `flink-shaded-guava 33.x` for Flink itself.

The trade-off is that the uber jar relocates Kafka Connect classes too, into `org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.*`. Our `RawDebeziumDeserializationSchema` and the `TableFilter` import the relocated `SourceRecord` / `Struct` from there. (The relocated path is what `DebeziumDeserializationSchema.deserialize(...)` actually expects in this build of the uber jar — adding `org.apache.kafka:connect-api` as a separate dep wouldn't satisfy that signature.)

Both `flink-sql-connector-postgres-cdc` and `flink-shaded-guava` are marked `provided`: in production the uber jar lives in the Flink cluster's `lib/` directory and `flink-shaded-guava 33.x` is pulled in by Flink core.

## Configuration

All connection details and the table list are read from environment variables. The defaults match `docker-compose.yml`, so running from the IDE without setting anything works out of the box.

| Variable | Default | Notes |
|---|---|---|
| `SOURCE_HOST` | `localhost` | |
| `SOURCE_PORT` | `35432` | |
| `SOURCE_DATABASE` | `source` | |
| `SOURCE_USER` | `flink` | |
| `SOURCE_PASSWORD` | `flink` | |
| `TARGET_HOST` | `localhost` | |
| `TARGET_PORT` | `35433` | |
| `TARGET_DATABASE` | `target` | |
| `TARGET_USER` | `flink` | |
| `TARGET_PASSWORD` | `flink` | |
| `TABLES` | `public.users,public.orders` | Comma-separated `schema.table` pairs. |
| `SLOT_NAME` | `flink_cdc_replication` | Postgres logical replication slot name. |
| `PARALLELISM` | `8` | Global Flink job parallelism (`env.setParallelism`). |
| `SINK_PARALLELISM` | `4` | Per-sink JDBC parallelism (`sink.parallelism` table option). |
| `CHECKPOINT_INTERVAL_MS` | `10000` | Checkpoint interval in milliseconds (`env.enableCheckpointing`). |

## Adding a new table

1. Create the table in `postgres-source` (and `postgres-target` if you want it replicated).
2. Add it to the `TABLES` env var (e.g. `TABLES=public.users,public.orders,public.invoices`).

No DDL, no POJO, no schema definition in Java — `PostgresCatalog` reads the column types and primary key from the source database at startup.
