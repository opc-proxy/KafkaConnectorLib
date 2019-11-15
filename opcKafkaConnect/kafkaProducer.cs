using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using OpcProxyClient; 
using Opc.Ua; 
using NLog;

using Avro;
using Avro.Generic;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;

namespace opcKafkaConnect
{
    public class opcKafkaProducer 
    {
        public IProducer<String,GenericRecord> producer;
        public Logger log;
        public CachedSchemaRegistryClient  schemaRegistry;
        public opcSchemas schemas;
        public kafkaProducerConf _conf;

        public opcKafkaProducer(kafkaProducerConf conf, CachedSchemaRegistryClient schemaRegistry){ 
            _conf = conf;

            // setup the logger
            log = LogManager.GetLogger(this.GetType().Name);

            // instance the List of schemas
            schemas = new opcSchemas();
            
            // instace producer with Avro serializers
            producer = new ProducerBuilder<string, GenericRecord>(_conf.getProducerConf())
            //.SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
            .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry))
            .SetErrorHandler((_, e) => log.Error($"Error: {e.Reason}"))
            .Build();
        }

        /// <summary>
        /// Event handler that fires any time there is a change in a OPC monitored variable
        /// </summary>
        /// <param name="emitter"></param>
        /// <param name="items"></param>
        public void OnNotification(object emitter, MonItemNotificationArgs items){

            RecordSchema schema = schemas.GetSchema(items.dataType);
            if(schema != null){
                foreach(var itm in items.values){
                    if(DataValue.IsBad(itm)) continue;
                    var m = buildKafkaMessage(itm,schema,items.dataType,items.name);
                    if(m ==null) continue;
                    // not waiting here
                    var status = sendMessage(_conf.opcSystemName,m);
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
                var value = Convert.ChangeType(data.Value, opcSchemas.getAvroSerializationType(dataType));
                record.Add("value", value);
                // record.Add("type", opcSchemas.getAvroType(dataType));  // don't need it
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
            catch(Exception e){
                log.Error($"Delivery failed: {e.Message}");
                return kafkaMessageStatus.Failed;
            }
        }
    }

    public enum kafkaMessageStatus{
        Delivered, Failed
    }
}
