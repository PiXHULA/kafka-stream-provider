# kafka-stream-provider

A small Kafka Streams app, running on Open Liberty, with REST endpoints to:

- submit new data onto an input Kafka topic (`POST /api/data`)
- read back everything the Kafka Streams app has consumed so far, via an
  interactive query against the Streams state store (`GET /api/data`)
- reset the stream's consumer group offsets back to 0 and restart it, so you
  can replay the topic from the beginning (`POST /api/admin/reset-offsets`)

## Architecture

- `KafkaStreamsManager` (CDI `@ApplicationScoped` bean) owns a `KafkaProducer` and a
  `KafkaStreams` instance for the whole app lifetime.
- The topology materializes the input topic as a `KTable` into a state store
  (`messages-store`). Every submitted message is sent with a random UUID key, so
  entries accumulate in the store instead of overwriting each other.
- `GET /api/data` queries that store directly (Kafka Streams "Interactive Queries").
- `POST /api/admin/reset-offsets` stops the stream, uses the Kafka `AdminClient` to
  reset the committed offsets of the app's consumer group back to `0` for every
  partition of the topic, clears local state (`KafkaStreams#cleanUp`), and starts a
  fresh `KafkaStreams` instance - which then replays the topic from the start.

## Running Kafka locally

A `docker-compose.yml` is provided with:

- `kafka`: a single-node Kafka broker running in KRaft mode (no Zookeeper needed),
  listening on `localhost:9094` from the host.
- `kafdrop`: a very lightweight web UI for browsing topics/messages (simpler
  alternative to Confluent Control Center), available at http://localhost:9000.

Start it with:

```powershell
docker compose up -d
```

## Running the app

```powershell
mvn liberty:dev
```

This compiles the app, starts Open Liberty, and deploys it with hot reload. The app
listens on http://localhost:9080.

Bootstrap servers/topic/application id default to the values in
`src/main/resources/META-INF/microprofile-config.properties` and can be overridden
with environment variables `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`,
`KAFKA_APPLICATION_ID`, `KAFKA_STATE_DIR`.

The Kafka Streams state store (RocksDB files/checkpoints) is kept under `kafka-streams-state/`
at the repo root (instead of the OS temp directory, which can be cleared at any time) - safe to
delete any time the app isn't running.

## Trying it out

> **Note for Windows/PowerShell users:** `curl` is aliased to `Invoke-WebRequest` in
> PowerShell, and PowerShell's own argument quoting mangles `\"`-escaped JSON bodies
> passed to native executables like real `curl.exe` - the JSON often arrives corrupted
> server-side (e.g. `Unexpected char 92`), gets rejected during deserialization, and
> your resource method (and its logs) never even gets invoked. Use `Invoke-RestMethod`
> instead, which handles the JSON body correctly:

```powershell
Invoke-RestMethod -Uri http://localhost:9080/api/data -Method Post -ContentType "application/json" -Body '{"message":"hello kafka"}'
Invoke-RestMethod -Uri http://localhost:9080/api/data -Method Post -ContentType "application/json" -Body '{"message":"another message"}'
```

On macOS/Linux/bash, the original `curl` form works fine:

```bash
curl -X POST http://localhost:9080/api/data -H "Content-Type: application/json" -d '{"message":"hello kafka"}'
curl -X POST http://localhost:9080/api/data -H "Content-Type: application/json" -d '{"message":"another message"}'
```

Read back everything the stream has consumed:

```powershell
Invoke-RestMethod -Uri http://localhost:9080/api/data
```

Reset the stream's offsets to 0 and watch it replay everything from the beginning:

```powershell
Invoke-RestMethod -Uri http://localhost:9080/api/admin/reset-offsets -Method Post
Invoke-RestMethod -Uri http://localhost:9080/api/data
```

Browse the topic/messages directly in Kafdrop: http://localhost:9000

