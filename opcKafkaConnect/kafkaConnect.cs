using System;
using System.Threading.Tasks;
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
    public class KafkaConnect : IOPCconnect
    {
        public IProducer<String,GenericRecord> producer;

        public serviceManager _serv;
        public Logger log;
        public kafkaProducerConf producer_conf;

        public CachedSchemaRegistryClient  schemaRegistry;

        public opcSchemas schemasHolder;

        public async void init(JObject config){ 
            // setup the logger
            log = LogManager.GetLogger(this.GetType().Name);

            // producer config
            producer_conf = config.ToObject<kafkaConfWrapper>().kafkaProducer;
            // consumer config
            
            // instance the schema registry
            schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig(){
                Url = producer_conf.SchemaRegistryURL, 
                ValueSubjectNameStrategy = SubjectNameStrategy.TopicRecord
            } );
            // This will crash if schema registry is offline FIXME, 
            // this part is only here to make sure the user has the schema registry up, otherwise
            // the error is misleading: "Delivery failed: Local: Key serialization error"
            var lst = await schemaRegistry.GetAllSubjectsAsync();

            // instance the List of schemas
            schemasHolder = new opcSchemas();
            
            // instace producer with Avro serializers
            producer = new ProducerBuilder<string, GenericRecord>(producer_conf._conf)
            .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
            .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry))
            .SetErrorHandler((_, e) => log.Error($"Error: {e.Reason}"))
            .Build();
            // instance consumer in new thread
        }

        /// <summary>
        /// Event handler that fires any time there is a change in a OPC monitored variable
        /// </summary>
        /// <param name="emitter"></param>
        /// <param name="items"></param>
        public void OnNotification(object emitter, MonItemNotificationArgs items){

            RecordSchema schema = schemasHolder.GetSchema(items.dataType);
            if(schema != null){
                foreach(var itm in items.values){
                    if(DataValue.IsBad(itm)) continue;
                    var m = buildKafkaMessage(itm,schema,items.dataType,items.name);
                    if(m ==null) continue;
                    // not waiting here
                    var status = sendMessage("balla",m);
                    log.Debug("Sending message {0}:{1}  t:{2}",m.Key,m.Value,m.Timestamp.ToString());
                }
            }
        }

        /// <summary>
        /// Build an Avro compatible Kafka message
        /// </summary>
        /// <param name="data">the dataValue</param>
        /// <param name="schema">Avro schema</param>
        /// <param name="dataType">Type of the data</param>
        /// <param name="name">Name of variable to be put as key</param>
        /// <returns></returns>
        public Message<String,GenericRecord> buildKafkaMessage(DataValue data, RecordSchema schema, Type dataType, string name){
            var time = new Timestamp(data.SourceTimestamp);
            var record = new GenericRecord(schema);
            try{
                // converting item value to the supported Avro type, only makes a difference for Int16
                var value = Convert.ChangeType(data.Value, opcSchemas.getAvroType(dataType));
                record.Add("value", value);
                var m = new Message<String,GenericRecord> {Value=record, Key=name, Timestamp=time};
                return m;            
            }
            catch(Exception e){
                log.Error("Cannot convert value " +data.Value.ToString() +" to " +dataType.ToString());
                log.Error(e.Message);
                return null;
            }
        }

        public async Task<kafkaMessageStatus> sendMessage(String topic, Message<String,GenericRecord> message){
            try{
                var deliveryResponse = await producer.ProduceAsync(topic, message);
                log.Debug("Delivered message {0}:{1}  t:{2}",deliveryResponse.Key,deliveryResponse.Value,deliveryResponse.Timestamp.UtcDateTime);
                return kafkaMessageStatus.Delivered;
            }
            catch(ProduceException<String,GenericRecord> e){
                log.Error($"Delivery failed: {e.Error.Reason}");
                return kafkaMessageStatus.Failed;
            }
        }
        public void setServiceManager(serviceManager serv){
            _serv = serv;
        }
    }

    public enum kafkaMessageStatus{
        Delivered, Failed
    }

    public class kafkaConfWrapper{
        public kafkaProducerConf kafkaProducer {get; set;}
        public kafkaConsumerConf kafkaConsumer {get; set;}
        public string opcSystemName;
        public kafkaConfWrapper(){
            kafkaProducer = new kafkaProducerConf();
            kafkaConsumer = new kafkaConsumerConf();
            opcSystemName = "OPC";
        }
    }
    /// <summary>
    /// You can find definition of all props in https://docs.confluent.io/current/clients/confluent-kafka-dotnet/api/Confluent.Kafka.ProducerConfig.html
    /// and 
    /// 
    /// </summary>
    public class kafkaProducerConf{
        public string SchemaRegistryURL {get; set;}
        public string BootstrapServers {get{return _conf.BootstrapServers;} set{_conf.BootstrapServers = value;}}
        public int? BatchNumMessages{get{return _conf.BatchNumMessages;} set{_conf.BatchNumMessages = value;}}
        
        // the LingerMs get has a bug load new version of library when is out:
        // https://github.com/confluentinc/confluent-kafka-dotnet/issues/1080
        public double? LingerMs{get{return _conf.LingerMs;} set{_conf.LingerMs = value;}} 
        public int? QueueBufferingMaxKbytes{get{return _conf.QueueBufferingMaxKbytes;} set{_conf.QueueBufferingMaxKbytes = value;}}
        public int? QueueBufferingMaxMessages{get{return _conf.QueueBufferingMaxMessages;} set{_conf.QueueBufferingMaxMessages = value;}}
        public int? MessageTimeoutMs{get{return _conf.MessageTimeoutMs;} set{_conf.MessageTimeoutMs = value;}}
        public bool? EnableIdempotence{get{return _conf.EnableIdempotence;} set{_conf.EnableIdempotence = value;}}
        public int? RetryBackoffMs{get{return _conf.RetryBackoffMs;} set{_conf.RetryBackoffMs = value;}}
        public int? MessageSendMaxRetries{get{return _conf.MessageSendMaxRetries;} set{_conf.MessageSendMaxRetries = value;}}

        public ProducerConfig _conf;
        public kafkaProducerConf(){
            // note that the default for ProducerConfig is set to null all props... Certainly not the best
            _conf = new ProducerConfig();
            BootstrapServers = "localhost:9092";
            LingerMs = 100;
            SchemaRegistryURL = "localhost:8081";
        }
    }
}
