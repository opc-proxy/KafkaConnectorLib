using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using OpcProxyClient; 
using Opc.Ua; 
using OpcProxyCore;
using Newtonsoft.Json.Linq;
using NLog;

namespace opcKafkaConnect
{
    public class KafkaConnect : IOPCconnect
    {
        public IProducer<String,String> producer;

        public serviceManager _serv;
        public Logger log;
        public kafkaProducerConf _conf;

        public void init(JObject config){ 
            log = LogManager.GetLogger(this.GetType().Name);

            _conf = config.ToObject<kafkaProducerConf>();
            producer = new ProducerBuilder<string, string>(_conf._conf)
            .SetErrorHandler((_, e) => log.Error($"Error: {e.Reason}"))
            .Build();
        }
        public void OnNotification(object emitter, MonItemNotificationArgs items){
            foreach(var itm in items.values){
                var time = new Timestamp(itm.SourceTimestamp); 
                var m = new Message<String,String> {Value=itm.Value.ToString(), Key=items.name, Timestamp=time};
                sendMessage("test-topic",m);
                log.Debug("Sending message {0}:{1}  t:{2}",m.Key,m.Value,m.Timestamp.ToString());
            }
        }
        public async void sendMessage(string topic, Message<String,String> message){
            try{
                var deliveryResponse = await producer.ProduceAsync(topic, message);
                log.Debug($"Delivered '{deliveryResponse.Value}' to '{deliveryResponse.TopicPartitionOffset}'");
            }
            catch(ProduceException<String,String> e){
                log.Error($"Delivery failed: {e.Error.Reason}");
            }
        }
        public void setServiceManager(serviceManager serv){
            _serv = serv;
        }
    }


    public class kafkaProducerConf{
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
            _conf = new ProducerConfig();
            BootstrapServers = "localhost:9092";
            LingerMs = 0.5;
        }
    }
}
