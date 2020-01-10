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
        public IConsumer<String,GenericRecord> _consumer;
        private IProducer<String,GenericRecord> _producer;
        private serviceManager _serv;
        private opcSchemas _schemas;
        private Logger logger;
        private kafkaRPCConf _conf;
        public opcKafkaRPC( kafkaRPCConf conf, CachedSchemaRegistryClient schemaRegistry ){
            _conf = conf;

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
        public void run(CancellationToken cancel){
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
                                
                                JResponse res = new JResponse();
                                try{
                                    JRequest req = validateRequest(consumeResult.Value);
                                    // at this point only write to opc is supported FIXME
                                    var status = await _serv.writeToOPCserver((string)req.parameters[0], req.parameters[1]);

                                    if(status != null && status.Count > 0 && StatusCode.IsGood(status[0])){
                                        res.setWriteResult(consumeResult, (string)req.parameters[1]);
                                        await sendResponse(res);
                                        logger.Debug("Successful Write action to opc-server and Response");
                                    }
                                    else {
                                        res.setError(consumeResult,"Error in writing to OPC server");
                                        await sendResponse(res);
                                    }
                                }
                                catch{
                                    logger.Error("Invalid Request, value: "+ consumeResult.Value);
                                    res.setError(consumeResult,"Invalid Request");
                                    await sendResponse(res);
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


            Task.Run(action,cancel);
            
        }

        /// <summary>
        /// Send asyncronously a response message on the response-stream.
        /// </summary>
        /// <param name="res"></param>
        public async Task<DeliveryResult<string,GenericRecord>> sendResponse(JResponse res){
            var record = res.getGenericRecord(_schemas);
            logger.Debug("Sending response on: {0}, with key {1}",_conf.opcSystemName + "-response", res.key  );
            return await _producer.ProduceAsync(_conf.opcSystemName + "-response",new Message<string, GenericRecord>{Key=res.key, Value=record});
        }

        public JRequest validateRequest(GenericRecord input){
            
            object method;
            object parameters;
            object[] parameters_array;
            object id;
            List<String> supported_methods = new List<String>{"write"};
            JRequest req = new JRequest();

            // Validate correct types
            input.TryGetValue("method", out method);
            if(method == null || method.GetType() != typeof(String)) throw new Exception("'method' field null or not a string");
            
            input.TryGetValue("params", out parameters);
            if( parameters == null || !parameters.GetType().IsArray) throw new Exception("'params' field null or not an array");
            parameters_array = (Object[])parameters;

            input.TryGetValue("id", out id);
            if( id == null ) id = -999;
            else if( id.GetType() != typeof(Int32)) throw new Exception("'id' field null or not a integer");

            // method not supported
            if(!supported_methods.Contains((String)method)) throw new Exception("method '"+method+"' is not supported.");

            // Check if fullfills the write method parameters
            if((String)method == "write") {
                if( parameters_array.Length != 2 ) throw new Exception("For method 'write', 'params' must have only 2 entry");
                if( parameters_array[0].GetType() != typeof(String)) throw new Exception("'params' is not an array of strings");
                if( parameters_array[1].GetType() != typeof(String)) throw new Exception("'params' is not an array of strings");
            }

            req.method = (String)method;
            req.parameters = parameters_array;
            req.id = (Int32)id;

            return req;
        }

        public class JRequest {
            public string method {get;set;}
            public object[] parameters {get;set;}
            public long id {get; set;}
            public JRequest(){
                method = "";
                parameters = new Object[]{};
                id = -999;
            }
        }

        public class JResponse{
            public string key {get;set;}
            public string result {get; set;}
            public string error_message {get; set;}
            public Int32 error_code {get; set;}
            public long id {get; set;}

            public JResponse(){
                result = null;
                error_message = null;
                error_code = -999;
                id = -999;
                key = "no-key";
            }
            public GenericRecord getGenericRecord(opcSchemas schemas){
                var res = new GenericRecord(schemas.rpcResponse);
                var error = new GenericRecord(schemas.rpcError);
                
                if(result != null) {
                    res.Add("result",result);
                    res.Add("error", null);
                }
                // if there is an error then overrides the result
                if(error_code != -999 || error_message != null) {
                    error.Add("message",error_message);
                    error.Add("code",error_code);
                    res.Add("error",error);
                    res.Add("result",null);
                }
                res.Add("id",id);
                return res;
            }
            /// <summary>
            /// Sets the response ID to the Request ID if exist or to the provided offset
            /// </summary>
            /// <param name="r"></param>
            /// <param name="offset"></param>
            public void trySetID(GenericRecord r, long offset){
                object identifier;
                r.TryGetValue("id",out identifier);
                if(identifier != null && identifier.GetType() == typeof(Int32) ) id = (Int32) identifier;
                else id = offset;
            }

            public void setError(ConsumeResult<string,GenericRecord> request, string error){
                key = request.Key;
                error_code = 10;
                error_message = error;
                trySetID(request.Value, request.Offset.Value);
            }
            public void setWriteResult(ConsumeResult<string,GenericRecord> request, string outcome){
                key = request.Key;
                trySetID(request.Value, request.Offset.Value);
                result = outcome;
            }

        }   
    }
}