# KafkaConnectorLib

This library add the ability to the opc-proxy to stream data to a kafka server. It supports:

- Sending data stream on a topic when opc-variables change (notification form opc-server)
- Bidirectional comunication with the PLC using an RPC protocol. The protocol supported is JSON-RPC-2.0.
- Read and Write of OPC variable to server trough Kafka producer/consumer client.

# Requirements
- Kafka server running with at least one broker.
- Confluent Schema Registry running with at least one end-point.
- On your client side you need support for kafka-producer/consumer and Avro serialization/deserialization library.

# Documentation

You can find full documentation at [opc-proxy.readthedocs.io](https://opc-proxy.readthedocs.io/en/latest/connectors.html#kafka)


# Add it to your project with nuGet

```bash
dotnet add package opcProxy.KafkaConnector 
```

# NodeJS Client Example

A minimal example client for NodeJS that implements a communication between this OPC-Proxy Connector
trough a Kafka server can be found [here](https://github.com/opc-proxy/OPC-Node-Client-Examples/tree/master/Examples/Kafka). 

