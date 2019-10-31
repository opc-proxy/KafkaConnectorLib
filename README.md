# KafkaConnectorLib

### Configuration
```javascript
{
    // details on meaning of parameters can be found in:
    // https://docs.confluent.io/current/clients/confluent-kafka-dotnet/api/Confluent.Kafka.ProducerConfig.html
    // below the one that can be modified and their default values.
    kafkaProducer : {
        BootstrapServers : "localhost:9092",
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
