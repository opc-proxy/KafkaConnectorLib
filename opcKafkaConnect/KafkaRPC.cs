using System;
using System.Threading.Tasks;
using System.Threading;
using Confluent.Kafka;
using OpcProxyClient; 
using Opc.Ua; 
using OpcProxyCore;
using Newtonsoft.Json.Linq;
using NLog;

using System.Collections.Generic;
using Avro;
using Avro.Generic;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using Confluent.Kafka.SyncOverAsync;

namespace opcKafkaConnect
{
    /// <summary>
    /// Class that instanciate a kafka consumer and producer for a kafka-RPC based comunication,
    /// the protocol used is JSON-RPC-2.0, the producer and consumer are optimized for low latency
    /// and can be configured independently from the main data-stream.
    /// </summary>
    public class opcKafkaRPC{
        private IConsumer<String,GenericRecord> _consumer;
        private IProducer<String,GenericRecord> _producer;
        private serviceManager _serv;
        private opcSchemas _schemas;
        private Logger logger;

        public opcKafkaRPC( kafkaRPCConf conf, CachedSchemaRegistryClient schemaRegistry ){
            logger = LogManager.GetLogger(this.GetType().Name);
            _schemas = new opcSchemas();

            // Override default GroupID if name exist
            if(conf.GroupId == "OPC") conf.GroupId = conf.opcSystemName;
            
            // build the consumer, and subscribe it to topic
            _consumer = new ConsumerBuilder<String, GenericRecord>(conf.getConsumerConf())
            .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync())
            .SetErrorHandler((_, e) => logger.Error($"Error: {e.Reason}"))
            .Build();
            _consumer.Subscribe( conf.opcSystemName + "-request");

            // build the producer for the responses
            var producer_conf = new ProducerConfig(){
                BootstrapServers = conf.BootstrapServers, 
                LingerMs = 5   // low latency response
            };
            _producer = new ProducerBuilder<String, GenericRecord>(producer_conf)
            .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry))
            .SetErrorHandler((_, e) => logger.Error($"Error: {e.Reason}"))
            .Build();
        }
        public void setManager(serviceManager m){
            _serv = m;
        }
        
        /// <summary>
        /// Starts a new thread where the consumer runs and waits for data
        /// </summary>
        /// <param name="cancel"> Cancellation token, to cancel this thread and close the consumer connection to the broker gracefully</param>
        public Task run(CancellationToken cancel){
            Action action = async () =>
            {
                try
                {
                    while(true)
                    {   
                        try
                        {
                            logger.Debug("consuming....");
                            try{
                                var consumeResult = _consumer.Consume(cancel);
                                logger.Debug("Received kafka message in topic:"+consumeResult.Topic + " key:"+consumeResult.Key + " value:"+ consumeResult.Value);
                                
                                JRequest req = validateRequest(consumeResult);

                                // Write to OPC and check status of write operation
                                /*var status = await _serv.writeToOPCserver(req.props[0], req.props[1]);
                                if(status != null && status.Count > 0 && StatusCode.IsGood(status[0])){

                                    GenericRecord r = new GenericRecord(schemas.ack_message);
                                    r.Add("key",consumeResult.Key);
                                    r.Add("value",consumeResult.Value);
                                    r.Add("timestamp",consumeResult.Timestamp.ToString());
                                    r.Add("kafkaTPO",consumeResult.Topic + "-"+ consumeResult.Partition.Value.ToString() + "-" +consumeResult.Offset.Value.ToString());
                                    r.Add("status","SUCCESS");
                                    var bla = _producer.ProduceAsync(systemName + "-Ack",new Message<string, GenericRecord>{Key="ack-test", Value=r});
                                }
                                else {
                                    GenericRecord r = new GenericRecord(schemas.error_message);
                                    r.Add("subsystem","name");
                                    r.Add("action","OPC");
                                    r.Add("message","ERROR");
                                    var bla = _producer.ProduceAsync("error",new Message<string, GenericRecord>{Key="error-test", Value=r});
                                }*/
                            }
                            catch(MessageNullException e){
                                logger.Error(e.Message);
                            }
                        }
                        catch (ConsumeException e)
                        {
                            logger.Error($"Consume error: {e.Error.Reason}");
                            // producer send error
                        }
                    }
                }
                catch(OperationCanceledException)
                {
                    logger.Debug("Operation canceled, Closing consumer.");
                    _consumer.Close();
                }
            };


            return Task.Run(action,cancel);
            
        }

        public JRequest validateRequest(ConsumeResult<String,GenericRecord> input){
            
            object method;
            object props;
            object[] props_array;
            object id;
            List<String> supported_methods = new List<String>{"write"};
            JRequest req = new JRequest();

            // Validate correct types
            input.Value.TryGetValue("method", out method);
            if(method == null || method.GetType() != typeof(String)) throw new Exception();
            
            input.Value.TryGetValue("props", out props);
            if( props == null || props.GetType() != typeof(Object[])) throw new Exception();
            props_array = (Object[])props;

            input.Value.TryGetValue("id", out id);
            if(id == null || id.GetType() != typeof(Int32)) throw new Exception();

            // method not supported
            if(!supported_methods.Contains((String)method)) throw new Exception();

            // Check if fullfills the write method props
            if((String)method == "write") {
                if( props_array.Length != 2 ) throw new Exception();
                if( props_array[0].GetType() != typeof(String)) throw new Exception();
                if( props_array[1].GetType() != typeof(String)) throw new Exception();
            }

            req.method = (String)method;
            req.props = props_array;
            req.id = (Int32)id;

            return req;
        }

        public class JRequest {
            public string method {get;set;}
            public object[] props {get;set;}
            public Int32 id {get; set;}
            public JRequest(){
                method = "";
                props = new Object[]{};
                id = -999;
            }
        }   
    }
}