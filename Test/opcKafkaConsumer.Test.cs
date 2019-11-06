using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using opcKafkaConnect;
using Newtonsoft.Json.Linq; 
using Confluent.Kafka;
using NLog;
using Opc.Ua; 
using Avro;
using Avro.Generic;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;

namespace Test
{
    public class opcKafkaConsumerTest
    {
        public IProducer<String,String> producer;
        public opcKafkaConsumer consumer;
        CancellationTokenSource cancel;

        public opcKafkaConsumerTest(){
            var log = new NLog.Config.LoggingConfiguration();
            var logconsole = new NLog.Targets.ColoredConsoleTarget("logconsole");

            // Rules for mapping loggers to targets            
            log.AddRule( LogLevel.Debug, LogLevel.Fatal, logconsole);
            // Apply config           
            NLog.LogManager.Configuration = log; 

            consumer = new opcKafkaConsumer(new kafkaConsumerConf(), "OPC");

            cancel = new CancellationTokenSource();

            producer = new ProducerBuilder<String, String>(new kafkaProducerConf()._conf).Build();

        }

        [Fact]
        public async void consumeTest()
        {
            // SHOULD:
            // - Read a produced stream of string
            // - Start reading from NOW, not read previously made messages
            var res = await producer.ProduceAsync("OPC-WriteTo",new Message<string,string>{Key="test", Value="ShouldNotDeliver"});
            
            Task<ConsumeResult<string,string>> t = Task.Factory.StartNew(() =>
                {
                    var h = consumer._consumer.Consume( cancel.Token ); 
                return h;
            });
            // wait initialization of consumer
            Thread.Sleep(1000);
            res = await producer.ProduceAsync("OPC-WriteTo",new Message<string,string>{Key="test", Value="ciao"});
            Console.WriteLine("Message Sent");
            t.Wait();
            var r = t.Result;
            Assert.Equal("ciao", r.Value);
        }
    }
}
