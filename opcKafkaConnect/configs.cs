using Confluent.Kafka;

namespace opcKafkaConnect{



    public class kafkaConfWrapper{
        public kafkaProducerConf kafkaProducer {get; set;}
        public kafkaRPCConf kafkaRPC {get; set;}
        public string opcSystemName {get {return this.opcSystemName;} set{this.opcSystemName = value; kafkaRPC.opcSystemName = value; }}
        public string KafkaSchemaRegistryURL {get; set;}
        public string KafkaServers {
            get{return this.KafkaServers;} 
            set{
                this.KafkaServers = value; 
                if(kafkaProducer.BootstrapServers == "localhost:9092") kafkaProducer.BootstrapServers = value;
                if(kafkaRPC.BootstrapServers == "localhost:9092") kafkaRPC.BootstrapServers = value;
            }
        }

        
        public kafkaConfWrapper(){
            kafkaProducer = new kafkaProducerConf();
            kafkaRPC = new kafkaRPCConf();
            opcSystemName = "OPC";
            KafkaSchemaRegistryURL = "localhost:8081";
            KafkaServers = "localhost:9092";
        }
    }

        public class kafkaRPCConf{
        private ConsumerConfig _conf;
        public string BootstrapServers {get{return _conf.BootstrapServers;} set{_conf.BootstrapServers = value;}}
        public string GroupId {get{return _conf.GroupId;} set{_conf.GroupId = value;}}

        public string opcSystemName;

        public ConsumerConfig getConsumerConf(){
            return _conf;
        }
        public kafkaRPCConf(){
            _conf = new ConsumerConfig();
            BootstrapServers = "localhost:9092";
            GroupId = "OPC";
            opcSystemName = "OPC";
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
    /// <summary>
    /// You can find definition of all props in https://docs.confluent.io/current/clients/confluent-kafka-dotnet/api/Confluent.Kafka.ProducerConfig.html
    /// and 
    /// 
    /// </summary>
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

        private ProducerConfig _conf;
        public ProducerConfig getProducerConf(){return _conf;}
        public kafkaProducerConf(){
            // note that the default for ProducerConfig is set to null all props... Certainly not the best
            _conf = new ProducerConfig();
            BootstrapServers = "localhost:9092";
            LingerMs = 100;
        }
    }

}