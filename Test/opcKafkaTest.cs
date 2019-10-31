using System;
using Xunit;
using opcKafkaConnect;
using Newtonsoft.Json.Linq; 
using Confluent.Kafka;
using NLog;

using Avro;
using Avro.Generic;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;

namespace Test
{
    public class opcKafkaTest
    {
        public KafkaConnect kafka;

        public opcKafkaTest(){
            var log = new NLog.Config.LoggingConfiguration();
            var logconsole = new NLog.Targets.ColoredConsoleTarget("logconsole");
            // Rules for mapping loggers to targets            
            log.AddRule( LogLevel.Debug, LogLevel.Fatal, logconsole);
            // Apply config           
            NLog.LogManager.Configuration = log; 


            kafka = new KafkaConnect();
            var conf = JObject.Parse(@"{
                kafkaProducer:{
                    MessageSendMaxRetries: 100,
                    BatchNumMessages:23,
                    QueueBufferingMaxKbytes:100,
                    QueueBufferingMaxMessages:32,
                    MessageTimeoutMs:10000,
                    LingerMs:200
                }
            }");
            kafka.init(conf);

        }

        [Fact]
        public void Init()
        {
            // config works
            Assert.Equal(100, kafka.producer_conf._conf.MessageSendMaxRetries);
            Assert.Equal(23, kafka.producer_conf._conf.BatchNumMessages);
            Assert.Equal(100, kafka.producer_conf._conf.QueueBufferingMaxKbytes);
            Assert.Equal(32,kafka.producer_conf._conf.QueueBufferingMaxMessages);
            Assert.Equal(10000, kafka.producer_conf._conf.MessageTimeoutMs);
        }
        [Fact]
        public async void str_message(){
            // Message schema
             RecordSchema string_schema = (RecordSchema)RecordSchema.Parse(@"
                {
                    ""type"": ""record"",
                    ""name"": ""str"",
                    ""fields"": [
                        {""name"": ""value"", ""type"": ""string""}
                    ]
                }");
            // fill the record
            var record = new GenericRecord(string_schema);
            record.Add("value","ciao");

            //sending message succeded
            var m = new Message<string,GenericRecord>{ Value=record, Key="hey", Timestamp=new Timestamp()};
            var status = await kafka.sendMessage("test-topic", m);
            Assert.Equal(kafkaMessageStatus.Delivered,status);
        }
        [Fact]
        public async void double_message(){
            // Message schema
             RecordSchema string_schema = (RecordSchema)RecordSchema.Parse(@"
                {
                    ""type"": ""record"",
                    ""name"": ""dbl"",
                    ""fields"": [
                        {""name"": ""value"", ""type"": ""double""}
                    ]
                }");
            // fill the record
            var record = new GenericRecord(string_schema);
            record.Add("value",1098.87);

            //sending message succeded
            var m = new Message<string,GenericRecord>{ Value=record, Key="yoyo", Timestamp=new Timestamp()};
            var status = await kafka.sendMessage("test-topic", m);
            Assert.Equal(kafkaMessageStatus.Delivered,status);
        }

        [Fact]
        public void AvroTest()
        {
            kafka.avroTest();
        }
    }
}
