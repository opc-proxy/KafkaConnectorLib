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


        public void init(JObject config, CancellationTokenSource cts){ 
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

            // this part is only here to make sure the user has the schema registry up, otherwise
            // the error is misleading: "Delivery failed: Local: Key serialization error"
            testSchemaRegistry();

            // instace producer with Avro serializers
            producer = new opcKafkaProducer(conf.kafkaProducer,schemaRegistry);

            // instance consumer in new thread
            kafkaRPC = new opcKafkaRPC(conf.kafkaRPC, schemaRegistry);
            kafkaRPC.setManager(_serv);
            if(conf.kafkaRPC.enableKafkaRPC) kafkaRPC.run(cancel.Token);   
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

        public async void testSchemaRegistry(){
            try{
                var lst = await schemaRegistry.GetAllSubjectsAsync();
            }
            catch(Exception e){
                log.Fatal("Problem in initializing Confluent Schema Registry: "+e.Message);
                cancel.Cancel();
            }
        }

        public void clean(){
            log.Debug("Flushing producer...");
            try{
                producer.producer.Flush(new TimeSpan(0,0,0,0,500));
            }
            catch(Exception e){
                log.Error("Producer failed to flush " + e.Message);
            }
        }
    }
}
