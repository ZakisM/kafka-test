const { Kafka, CompressionTypes, logLevel } = require("kafkajs");
const { SchemaRegistry, SchemaType } = require("@kafkajs/confluent-schema-registry");

// Mock the external dependencies
jest.mock("kafkajs");
jest.mock("@kafkajs/confluent-schema-registry");

// Import the module to be tested
const producerApp = require("./producer"); // Assuming your producer logic is encapsulated or can be triggered

describe("Kafka Producer", () => {
  let mockProducer;
  let mockRegistry;

  beforeEach(() => {
    // Reset mocks before each test
    jest.clearAllMocks();

    // Setup mock implementations
    mockProducer = {
      connect: jest.fn().mockResolvedValue(undefined),
      send: jest.fn().mockResolvedValue({ messageId: "mockMessageId" }),
      disconnect: jest.fn().mockResolvedValue(undefined),
    };
    Kafka.prototype.producer = jest.fn().mockReturnValue(mockProducer);

    mockRegistry = {
      register: jest.fn().mockResolvedValue({ id: 123 }),
      encode: jest.fn((id, payload) => Buffer.from(JSON.stringify(payload) + "-encoded")), // Mock encoding
    };
    SchemaRegistry.mockImplementation(() => mockRegistry);
  });

  // Test producer connection (implicitly tested by run, but good to have a focus)
  test("should connect the producer and register schema on run", async () => {
    // producer.js runs `run()` automatically at the end of the file.
    // We need to re-require it to trigger the run() function in the test environment
    // or export run() and call it. For simplicity, let's assume run can be called.
    // If producer.js is structured to run immediately, this test will be tricky without refactoring producer.js
    // For now, let's assume `run` is exported from producer.js or can be triggered.
    // If not, we would need to refactor producer.js to export `run` or relevant functions.

    // To test the main execution flow, we can re-require the module.
    // This is a common pattern for testing scripts that execute on load.
    jest.isolateModules(() => {
      require("./producer");
    });

    // Wait for promises to resolve if run() is async and not immediately finishing
    await new Promise(process.nextTick); // Allow microtasks like connect/register to complete

    expect(mockProducer.connect).toHaveBeenCalledTimes(1);
    expect(mockRegistry.register).toHaveBeenCalledTimes(1);
    expect(mockRegistry.register).toHaveBeenCalledWith({
      type: SchemaType.AVRO,
      schema: JSON.stringify({
        type: "record",
        name: "RandomNumber",
        fields: [
          { name: "key", type: "string" },
          { name: "value", type: "string" },
        ],
      }),
    });
  });


  test("sendMessage should call producer.send with encoded message", async () => {
    // This test requires `run` to have completed and `registryId` to be set.
    // We'll simulate this by setting it manually after module load.
    let producerModule;
    jest.isolateModules(() => {
      producerModule = require("./producer");
    });
    await new Promise(process.nextTick); // allow run() to complete

    // Manually set registryId for testing sendMessage directly if needed,
    // or ensure `run` correctly sets it.
    // producerModule.registryId = 123; // This would require exporting registryId or making it settable

    // Trigger sendMessage (it's called via setInterval in producer.js, so we'll call it directly)
    // To do this, sendMessage would need to be exported from producer.js
    // If it's not, we can't test it in isolation easily without refactoring.

    // Assuming sendMessage is part of an interval, let's test the behavior
    // by checking calls to producer.send after the interval would have fired.
    // We can use Jest's fake timers for this.
    jest.useFakeTimers();

    // Re-require and run with fake timers
    jest.isolateModules(() => {
        require("./producer");
    });
    await new Promise(process.nextTick); // connect and register

    // Fast-forward time to trigger sendMessage via setInterval
    jest.advanceTimersByTime(3000); // Advance by 3 seconds

    await new Promise(process.nextTick); // Allow sendMessage async operations to complete

    expect(mockProducer.send).toHaveBeenCalledTimes(1);
    expect(mockRegistry.encode).toHaveBeenCalledTimes(1); // Ensure encode was called

    const expectedOriginalPayload = expect.objectContaining({
        key: expect.stringMatching(/^key-\d+$/),
        value: expect.stringMatching(/^value-\d+-/),
    });
    expect(mockRegistry.encode).toHaveBeenCalledWith(123, expectedOriginalPayload);

    // Check that producer.send was called with the result of registry.encode
    expect(mockProducer.send).toHaveBeenCalledWith(expect.objectContaining({
        messages: expect.arrayContaining([
            expect.objectContaining({
                value: Buffer.from(JSON.stringify(mockRegistry.encode.mock.calls[0][1]) + "-encoded"),
            })
        ]),
        compression: CompressionTypes.GZIP,
        topic: "individual-record",
    }));

    jest.useRealTimers(); // Restore real timers
  });

  // Test disconnection on signals (simplified example)
  // This requires more complex setup to simulate process signals.
  // For now, we'll assume the signal handling in producer.js is standard.
  // A simple test could be to ensure disconnect is called if process.emit is used.
});
