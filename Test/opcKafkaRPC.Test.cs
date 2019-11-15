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
using Confluent.Kafka.SyncOverAsync;

namespace Test
{
    [Collection("Sequential")]
    public class opcKafkaRPCTest
    {
        public IProducer<String,GenericRecord> producer;
        public IConsumer<String,GenericRecord> consumer;

        public opcKafkaRPC rpc;
        CancellationTokenSource cancel;

        CachedSchemaRegistryClient registry;

        opcSchemas schemas;

        GenericRecord req;

        public opcKafkaRPCTest(){
            var log = new NLog.Config.LoggingConfiguration();
            var logconsole = new NLog.Targets.ColoredConsoleTarget("logconsole");

            // Rules for mapping loggers to targets            
            log.AddRule( LogLevel.Debug, LogLevel.Fatal, logconsole);
            // Apply config           
            NLog.LogManager.Configuration = log; 
            
            // schema registry
            registry = new CachedSchemaRegistryClient(new SchemaRegistryConfig(){
                Url = "localhost:8081",
                ValueSubjectNameStrategy = SubjectNameStrategy.TopicRecord
            } );
            // default configuration
            rpc = new opcKafkaRPC(new kafkaRPCConf(),registry);

            cancel = new CancellationTokenSource();

            schemas = new opcSchemas();

            producer = new ProducerBuilder<String, GenericRecord>(new kafkaProducerConf().getProducerConf())
                .SetValueSerializer(new AvroSerializer<GenericRecord>(registry))
                .Build();

            req = new GenericRecord(schemas.rpcRequest);
            req.Add("method","write");
            req.Add("params",new string[]{"ciao","bella"});
            req.Add("id", 89);

            var cnf = new kafkaRPCConf().getConsumerConf();
            cnf.AutoOffsetReset = AutoOffsetReset.Earliest;
            
            // this is necessary otherwise fail because of rebalancing, send messages to dead consumer
            cnf.GroupId = "test-never-give-this-id-" + DateTime.Now.Millisecond.ToString();
            Console.WriteLine("using group id: " + "test-never-give-this-id-" + DateTime.Now.Millisecond.ToString() );

            consumer = new ConsumerBuilder<String, GenericRecord>(cnf)
            .SetValueDeserializer(new AvroDeserializer<GenericRecord>(registry).AsSyncOverAsync())
            .Build();

            consumer.Subscribe( "OPC-response");
        }

        [Fact]
        public void InitTest()
        {
            // here some test about config... To be done FIXME
        }

        [Fact]
        public void requestValidation()
        {
            Console.WriteLine("Validation test");

            // should not throw
            var r = rpc.validateRequest(req);
            
            req.Add("params",new object[]{78,89});
            Assert.Throws<Exception>(()=>{rpc.validateRequest(req);});
            req.Add("params", 67);
            Assert.Throws<Exception>(()=>{rpc.validateRequest(req);});
            req.Add("params",new string[]{"ciao"});
            Assert.Throws<Exception>(()=>{rpc.validateRequest(req);});
            req.Add("params",new string[]{"ciao","bella"});

            req.Add("method","writ");
            Assert.Throws<Exception>(()=>{rpc.validateRequest(req);});
            req.Add("method", 90);
            Assert.Throws<Exception>(()=>{rpc.validateRequest(req);});
            req.Add("method","write");

            req.Add("id", "90");
            Assert.Throws<Exception>(()=>{rpc.validateRequest(req);});

        }


        [Fact]
        public async void consumeTest()
        {
            Console.WriteLine("Consume test");

            // Test correct init of consumer:
            // - Consumer with default init should read a produced stream on request
            // - in the config the consumer will start read from NOW is no offset is committed yet.
            // this is why I need to start the consumer first

            Task<ConsumeResult<string,GenericRecord>> t = Task.Factory.StartNew(() =>
                {
                    var h = rpc._consumer.Consume( cancel.Token ); 
                return h;
            });

            // wait initialization of consumer
            while(rpc._consumer.Assignment.Count==0){
                    Thread.Sleep(100);
            }
            Thread.Sleep(1000);
            var res = await producer.ProduceAsync("OPC-request",new Message<string,GenericRecord>{Key="ciao", Value=req});
            //Console.WriteLine("Message Sent");
            t.Wait();
            var r = t.Result;   
            Assert.Equal("ciao", r.Key);
            
        }


        [Fact]
        public async void ResponseTest()
        {
            Console.WriteLine("Resp test");
            var res = new opcKafkaRPC.JResponse(){ result = "89", id=67, key = "resp" };
            await rpc.sendResponse(res);
            var h = consumer.Consume( cancel.Token );
            Assert.Equal("resp", h.Key);

        }
    }
}
