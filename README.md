# Kafka Test Application

This application demonstrates a Kafka producer and consumer setup using `kafkajs` and Confluent Schema Registry for Avro schema management.

## Dependencies

The core dependencies for this project are:

-   `kafkajs`: A modern Apache Kafka client for Node.js.
-   `@kafkajs/confluent-schema-registry`: A KafkaJS plugin for Confluent Schema Registry. It handles the encoding and decoding of messages using schemas stored in the registry.
-   `avsc`: A library for working with Avro schemas and data. Used by the consumer for manual deserialization when schema validation is disabled.

## Configuration

The application can be configured using the following environment variables:

### Common

-   `SCHEMA_REGISTRY_HOST`: Specifies the URL of the Confluent Schema Registry.
    -   Example: `SCHEMA_REGISTRY_HOST=http://localhost:8081`
    -   Default: `http://localhost:8081`

### Consumer (`src/consumer.js`)

-   `VALIDATE_PAYLOADS`: Controls how the consumer handles message deserialization.
    -   `true` (default): The consumer uses `registry.decode()` to deserialize and validate messages against the schema in the registry. If validation fails, an error will be logged.
    -   `false`: The consumer bypasses `registry.decode()` and instead:
        1.  Extracts the schema ID from the message.
        2.  Fetches the schema directly from the registry using `registry.getSchema(id)`.
        3.  Uses `avsc` to deserialize the message payload according to the fetched schema. This allows for consuming messages that might have slight deviations acceptable by Avro (e.g., missing optional fields with defaults) but might be rejected by strict validation.
    -   Example: `VALIDATE_PAYLOADS=false`

## Running the Application

1.  **Start Kafka and Schema Registry:** Ensure your Kafka brokers and Confluent Schema Registry are running.
2.  **Install Dependencies:**
    ```bash
    npm install
    ```
3.  **Run the Producer:**
    ```bash
    npm run producer
    ```
4.  **Run the Consumer:**
    ```bash
    npm run consumer
    ```
    (You can set `SCHEMA_REGISTRY_HOST` and `VALIDATE_PAYLOADS` as environment variables before running.)

## Schema Example

The producer (`src/producer.js`) currently uses the following Avro schema for messages on the `individual-record` topic:

```json
{
  "type": "record",
  "name": "RandomNumber",
  "fields": [
    { "name": "key", "type": "string" },
    { "name": "value", "type": "string" }
  ]
}
```

This schema is registered with the Schema Registry under the subject `individual-record-value` (by default, subject naming strategy might vary).

## Testing

To run the automated tests:

```bash
npm test
```

This will execute Jest tests defined in `*.test.js` files within the `src` directory. The tests cover producer and consumer logic, including schema registration, message encoding/decoding, and conditional validation.
