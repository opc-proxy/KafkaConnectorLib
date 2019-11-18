using System;
using System.Threading.Tasks;
using System.Threading;
using OpcProxyClient; 
using OpcProxyCore;
using Newtonsoft.Json.Linq;
using NLog;

using Confluent.SchemaRegistry;

namespace opcKafkaConnect
{
    public class KafkaConnect : IOPCconnect
    {
        public serviceManager _serv;
        public Logger log;
        public CachedSchemaRegistryClient  schemaRegistry;
        public opcKafkaRPC kafkaRPC;
        public opcKafkaProducer producer;
        CancellationTokenSource cancel;


        public async void init(JObject config, CancellationTokenSource cts){ 
            // setup the logger
            log = LogManager.GetLogger(this.GetType().Name);
            kafkaConfWrapper conf = config.ToObject<kafkaConfWrapper>();
            // producer config
            var producer_conf = conf.kafkaProducer;
            cancel = cts;

            // instance the schema registry
            schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig(){
                Url = conf.KafkaSchemaRegistryURL, 
                ValueSubjectNameStrategy = SubjectNameStrategy.TopicRecord
            } );

            // This will crash if schema registry is offline FIXME, 
            // this part is only here to make sure the user has the schema registry up, otherwise
            // the error is misleading: "Delivery failed: Local: Key serialization error"
            var lst = await schemaRegistry.GetAllSubjectsAsync();

            // instace producer with Avro serializers
            producer = new opcKafkaProducer(conf.kafkaProducer,schemaRegistry);

            // instance consumer in new thread
            kafkaRPC = new opcKafkaRPC(conf.kafkaRPC, schemaRegistry);
            kafkaRPC.setManager(_serv);
            kafkaRPC.run(cancel.Token);
            
        }

        /// <summary>
        /// Event handler that fires any time there is a change in a OPC monitored variable
        /// </summary>
        /// <param name="emitter"></param>
        /// <param name="items"></param>
        public void OnNotification(object emitter, MonItemNotificationArgs items){
            producer.OnNotification(emitter,items);
        }

        public void setServiceManager(serviceManager serv){
            _serv = serv;
        }
    }
}
