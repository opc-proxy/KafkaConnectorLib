# KafkaConnectorLib

This library add the ability to the opc-proxy to stream data to a kafka server. It supports:

- Sending data stream on a topic when opc-variables change (notification form opc-server)
- Bidirectional comunication with the PLC using an RPC protocol. The protocol supported is JSON-RPC-2.0.
- Read and Write of OPC variable to server trough Kafka producer/consumer client.

### Requirements
- Kafka server running with at least one broker.
- Confluent Schema Registry running with at least one end-point.
- On your client side you need support for kafka-producer/consumer and Avro serialization/deserialization library.

### Configuration
```javascript
{
    // details on meaning of parameters for the Producer can be found in:
    // https://docs.confluent.io/current/clients/confluent-kafka-dotnet/api/Confluent.Kafka.ProducerConfig.html
    // below the one that can be modified and their default values.
    kafkaProducer : {
        BootstrapServers :  "localhost:9092", // comma separated list of kafka-brokers URLs
        SchemaRegistryURL : "localhost:8081", // comma separated list of URLs for the schema-registry server
        LingerMs : 100,
        BatchNumMessages : 10000,
        QueueBufferingMaxKbytes : 1048576,
        QueueBufferingMaxMessages : 100000,
        MessageTimeoutMs : 300000,
        EnableIdempotence : false,
        MessageSendMaxRetries : 2,
        RetryBackoffMs : 100
    }
}
```

### Data Serialization

### RPC Protocol Trough Kafka

