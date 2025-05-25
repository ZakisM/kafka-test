const { Kafka, logLevel } = require("kafkajs");
const { SchemaRegistry, MAGIC_BYTE } = require("@kafkajs/confluent-schema-registry");
const avsc = require("avsc");

// Mock external dependencies
jest.mock("kafkajs");
jest.mock("@kafkajs/confluent-schema-registry");
jest.mock("avsc");

// Store original process.env
const originalEnv = process.env;

describe("Kafka Consumer", () => {
  let mockConsumer;
  let mockRegistry;
  let mockAvscType;

  const mockConsoleLog = jest.spyOn(console, "log").mockImplementation(() => {});
  const mockConsoleError = jest.spyOn(console, "error").mockImplementation(() => {});
  const mockConsoleWarn = jest.spyOn(console, "warn").mockImplementation(() => {});


  beforeEach(() => {
    jest.resetModules(); // Important to reset modules to re-evaluate process.env
    process.env = { ...originalEnv }; // Reset process.env for each test

    mockConsumer = {
      connect: jest.fn().mockResolvedValue(undefined),
      subscribe: jest.fn().mockResolvedValue(undefined),
      run: jest.fn(async ({ eachMessage }) => {
        // Store the eachMessage handler to simulate message reception
        mockConsumer.eachMessage = eachMessage;
      }),
      disconnect: jest.fn().mockResolvedValue(undefined),
    };
    Kafka.prototype.consumer = jest.fn().mockReturnValue(mockConsumer);

    mockRegistry = {
      decode: jest.fn(),
      getSchema: jest.fn(),
    };
    SchemaRegistry.mockImplementation(() => mockRegistry);
    // Provide a default for MAGIC_BYTE if not already mocked by the library mock itself
    if (SchemaRegistry.MAGIC_BYTE === undefined) {
        SchemaRegistry.MAGIC_BYTE = 0;
    }


    mockAvscType = {
      fromBuffer: jest.fn(),
    };
    avsc.Type = {
      forSchema: jest.fn().mockReturnValue(mockAvscType),
    };
  });

  afterEach(() => {
    jest.clearAllMocks();
    process.env = originalEnv; // Restore original process.env
    mockConsoleLog.mockClear();
    mockConsoleError.mockClear();
    mockConsoleWarn.mockClear();
  });

  const simulateMessage = async (messagePayload) => {
    // Ensure run() has been called and eachMessage is available
    if (!mockConsumer.eachMessage) {
      throw new Error("Consumer run() was not called or eachMessage not set up.");
    }
    await mockConsumer.eachMessage({
      topic: "test-topic",
      partition: 0,
      message: {
        offset: "1",
        timestamp: new Date().toISOString(),
        key: Buffer.from("test-key"),
        value: messagePayload,
      },
    });
  };

  describe("Scenario 1: Validation Enabled (VALIDATE_PAYLOADS=true or not set)", () => {
    beforeEach(() => {
      // Ensure VALIDATE_PAYLOADS is true or not set (default behavior)
      delete process.env.VALIDATE_PAYLOADS;
      jest.isolateModules(() => { require("./consumer"); }); // Re-require to pick up env
    });

    test("should call registry.decode and process the decoded payload", async () => {
      const decodedPayload = { data: "test data" };
      mockRegistry.decode.mockResolvedValue(decodedPayload);
      const rawPayload = Buffer.from("raw-payload-bytes");

      await mockConsumer.connect(); // Ensure connect and run are called
      await mockConsumer.subscribe();
      await mockConsumer.run({ eachMessage: mockConsumer.eachMessage}); // Simulates the app calling run

      await simulateMessage(rawPayload);

      expect(mockRegistry.decode).toHaveBeenCalledWith(rawPayload);
      expect(mockConsoleLog).toHaveBeenCalledWith(expect.stringContaining(JSON.stringify(decodedPayload)));
      expect(mockRegistry.getSchema).not.toHaveBeenCalled();
      expect(avsc.Type.forSchema).not.toHaveBeenCalled();
    });
  });

  describe("Scenario 2: Validation Disabled (VALIDATE_PAYLOADS=false)", () => {
    beforeEach(() => {
      process.env.VALIDATE_PAYLOADS = "false";
      jest.isolateModules(() => { require("./consumer"); }); // Re-require to pick up env
    });

    test("should use avsc to decode and process the payload", async () => {
      const schemaId = 123;
      const avroSchema = { type: "record", name: "TestSchema", fields: [{ name: "data", type: "string" }] };
      const decodedPayload = { data: "avsc decoded data" };
      const actualPayloadBytes = Buffer.from("actual-payload-data");

      // Construct payload with magic byte and schema ID
      const buffer = Buffer.alloc(5 + actualPayloadBytes.length);
      buffer.writeUInt8(MAGIC_BYTE || 0, 0); // Use mocked MAGIC_BYTE or default
      buffer.writeInt32BE(schemaId, 1);
      actualPayloadBytes.copy(buffer, 5);

      mockRegistry.getSchema.mockResolvedValue({ schema: JSON.stringify(avroSchema) });
      mockAvscType.fromBuffer.mockReturnValue(decodedPayload);

      await mockConsumer.connect();
      await mockConsumer.subscribe();
      await mockConsumer.run({ eachMessage: mockConsumer.eachMessage});

      await simulateMessage(buffer);

      expect(mockRegistry.decode).not.toHaveBeenCalled();
      expect(mockRegistry.getSchema).toHaveBeenCalledWith(schemaId);
      expect(avsc.Type.forSchema).toHaveBeenCalledWith(avroSchema);
      expect(mockAvscType.fromBuffer).toHaveBeenCalledWith(actualPayloadBytes);
      expect(mockConsoleLog).toHaveBeenCalledWith(expect.stringContaining(JSON.stringify(decodedPayload)));
    });
  });

  describe("Scenario 3: Invalid payload with validation enabled", () => {
    beforeEach(() => {
      delete process.env.VALIDATE_PAYLOADS;
      jest.isolateModules(() => { require("./consumer"); });
    });

    test("should handle error gracefully when registry.decode throws an error", async () => {
      const decodeError = new Error("Validation Failed");
      mockRegistry.decode.mockRejectedValue(decodeError);
      const rawPayload = Buffer.from("invalid-raw-payload");

      await mockConsumer.connect();
      await mockConsumer.subscribe();
      await mockConsumer.run({ eachMessage: mockConsumer.eachMessage});

      // In the consumer, errors from registry.decode are caught by the top-level catch in run()
      // or by the process.on('unhandledRejection') if not caught by run's try/catch.
      // The current consumer.js catches errors in `run()` and logs `[example/consumer] ${e.message}`
      // However, the `eachMessage` itself doesn't have a try-catch for `registry.decode`.
      // Let's assume the global error handlers or the run() catch will handle this.
      // For a more direct test, `eachMessage` would need its own try-catch.

      // To test this properly, we might need to spy on console.error if that's where errors are logged.
      // The current consumer code has a top-level catch for `run()`
      // `run().catch((e) => console.error(`[example/consumer] ${e.message}`, e));`
      // And also process error handlers.
      // If `registry.decode` fails, the error propagates up.

      // We'll simulate the message and check if console.error was called.
      // This relies on the `run().catch()` or global error handlers in consumer.js
      
      // To make this testable without relying on process-wide handlers,
      // we'd ideally refactor consumer.js so `eachMessage` has its own try/catch.
      // Given the current structure, we expect the error to be caught by the main `run().catch()`.
      // However, jest's environment might not fully replicate that behavior for an async iterator.
      // Let's assume the error will be logged by the `eachMessage` internal try-catch if it were present,
      // or by the `run`'s catch block.

      // The current `eachMessage` in consumer.js does *not* have a try-catch around `registry.decode`.
      // So an error will make `eachMessage` reject, and `consumer.run` handles this.
      // The provided consumer.js has:
      // `await consumer.run({ eachMessage: async (...) => { ... } })`
      // Kafkajs handles errors in `eachMessage` and stops the consumer if it's not handled.
      // Let's assume `console.error` is called from the `run().catch` or signal handlers.

      try {
        await simulateMessage(rawPayload);
      } catch (e) {
        // This catch block might not be reached if Kafkajs handles the error internally
        // and logs it, then potentially stops the consumer.
        expect(e).toBe(decodeError); // Check if the error is the one we threw
      }
      // More robust: check if the error was logged as the consumer.js does
      // This depends on how consumer.js is structured to handle errors from eachMessage.
      // The consumer.js has `run().catch(e => console.error(...))`. This should catch it.
      // However, the `consumer.run` itself might also log.

      // Let's check the console.error mock, as that's the most likely place for it to appear.
      // The actual error logging might be complex due to Kafkajs internal error handling.
      // For this test, we expect the main `run().catch()` to log the error.
      // We need to call the entire run sequence.
      const consumerModule = require("./consumer"); // This calls run()
      await new Promise(process.nextTick); // Allow async operations in run to proceed

      try {
          await simulateMessage(rawPayload);
      } catch(e) {
          // Error might be handled by kafkajs, preventing it from reaching here
      }

      // Check if console.error was called (from the main catch or unhandledRejection)
      // This is an indirect way of checking, assuming the app's error handling works.
      // A better test would be to have a try/catch inside eachMessage in the app code.
      // Since it's not there, we rely on higher-level catches.
      // The error will be logged by the `run().catch(e => ...)` or `process.on('unhandledRejection')`
      
      // The current consumer.js will log it via the `run().catch` if the error from `eachMessage`
      // propagates out of `consumer.run`.
      // Or, if `consumer.run` handles it and logs, that's also an option.
      // Given the structure, we assume the main `run().catch` catches it.
      // To ensure this test works, let's assume the error from `registry.decode`
      // inside `eachMessage` will cause `run()` to reject.
      
      // Re-require and run to attach the catch block
      delete require.cache[require.resolve('./consumer')]; // remove from cache
      const p = require('./consumer'); // This will execute run()
      
      // We need to wait for the consumer to be ready to process messages
      await mockConsumer.connect();
      await mockConsumer.subscribe();
      await mockConsumer.run({ eachMessage: mockConsumer.eachMessage }); // Ensure eachMessage is set

      await simulateMessage(rawPayload); // This will cause decode to throw
      
      // Allow promise rejections to be processed
      await new Promise(setImmediate); 

      expect(mockConsoleError).toHaveBeenCalled();
      expect(mockConsoleError.mock.calls.some(call => call[0].includes("Validation Failed"))).toBe(true);
    });
  });

  describe("Scenario 4: Potentially 'invalid' payload with validation disabled", () => {
    beforeEach(() => {
      process.env.VALIDATE_PAYLOADS = "false";
      jest.isolateModules(() => { require("./consumer"); });
    });

    test("should process payload even if it has schema deviations, if avsc allows", async () => {
      const schemaId = 789;
      // Schema expects 'data' and 'optionalData'. Payload only provides 'data'.
      const avroSchema = {
        type: "record",
        name: "FlexibleSchema",
        fields: [
          { name: "data", type: "string" },
          { name: "optionalData", type: ["null", "string"], default: null },
        ],
      };
      // Payload is "valid" according to Avro rules if optionalData has a default
      const decodedPayloadViaAvsc = { data: "main data only", optionalData: null }; // avsc will fill default
      const actualPayloadBytes = avsc.Type.forSchema(avroSchema).toBuffer({ data: "main data only" }); // Simulate a payload missing optionalData

      const buffer = Buffer.alloc(5 + actualPayloadBytes.length);
      buffer.writeUInt8(MAGIC_BYTE || 0, 0);
      buffer.writeInt32BE(schemaId, 1);
      actualPayloadBytes.copy(buffer, 5);

      mockRegistry.getSchema.mockResolvedValue({ schema: JSON.stringify(avroSchema) });
      // Mock fromBuffer to return what Avro would if it successfully decodes (filling defaults)
      avsc.Type.forSchema.mockReturnValue({ fromBuffer: jest.fn().mockReturnValue(decodedPayloadViaAvsc) });


      await mockConsumer.connect();
      await mockConsumer.subscribe();
      await mockConsumer.run({ eachMessage: mockConsumer.eachMessage});

      await simulateMessage(buffer);

      expect(mockRegistry.decode).not.toHaveBeenCalled();
      expect(mockRegistry.getSchema).toHaveBeenCalledWith(schemaId);
      expect(avsc.Type.forSchema).toHaveBeenCalledWith(avroSchema);
      expect(avsc.Type.forSchema().fromBuffer).toHaveBeenCalledWith(actualPayloadBytes);
      expect(mockConsoleLog).toHaveBeenCalledWith(expect.stringContaining(JSON.stringify(decodedPayloadViaAvsc)));
    });
  });
});
