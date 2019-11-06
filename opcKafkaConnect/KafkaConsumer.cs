using System;
using System.Threading.Tasks;
using System.Threading;
using Confluent.Kafka;
using OpcProxyClient; 
using Opc.Ua; 
using OpcProxyCore;
using Newtonsoft.Json.Linq;
using NLog;

using Avro;
using Avro.Generic;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;

namespace opcKafkaConnect
{
    public class opcKafkaConsumer{
        public IConsumer<String,String> _consumer;
        private IProducer<String,GenericRecord> _producer;
        private serviceManager _serv;

        private opcSchemas schemas;
        private Logger logger;
        public string systemName ;
        public opcKafkaConsumer(kafkaConsumerConf config, string system_name){
            logger = LogManager.GetLogger(this.GetType().Name);
            schemas = new opcSchemas();
            systemName = system_name;
            // Override default GroupID if name exist
            if(config.GroupId == "OPC") config.GroupId = systemName;

            _consumer = new ConsumerBuilder<String, String>(config._conf)
            .SetErrorHandler((_, e) => logger.Error($"Error: {e.Reason}"))
            .Build();
            _consumer.Subscribe( systemName + "-WriteTo");

        }
        public void setManager(serviceManager m){
            _serv = m;
        }
        public void setProducer(IProducer<String,GenericRecord> p){
            _producer = p;
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
                                
                                // Write to OPC and check status of write operation
                                var status = await _serv.writeToOPCserver(consumeResult.Key, consumeResult.Value);
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
                                }
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
    }

    public class kafkaConsumerConf{
        public ConsumerConfig _conf {get; set;}
        public string BootstrapServers {get{return _conf.BootstrapServers;} set{_conf.BootstrapServers = value;}}
        public string GroupId {get{return _conf.GroupId;} set{_conf.GroupId = value;}}

        public kafkaConsumerConf(){
            _conf = new ConsumerConfig();
            BootstrapServers = "localhost:9092";
            GroupId = "OPC";
            // Necessary behaviour for OPC WRITE
            _conf.EnableAutoCommit = true;
            _conf.EnableAutoOffsetStore = true;
            _conf.AutoCommitIntervalMs = 100;
            _conf.SessionTimeoutMs = 6000;
            _conf.AutoOffsetReset = AutoOffsetReset.Latest;
            _conf.EnablePartitionEof = false;
            _conf.FetchWaitMaxMs = 0;
        }
    }
}