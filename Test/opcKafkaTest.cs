using System;
using Xunit;
using opcKafkaConnect;
using Newtonsoft.Json.Linq; 
using Confluent.Kafka;
using NLog;

namespace Test
{
    public class opcKafkaTest
    {
        public KafkaConnect kafka;

        public opcKafkaTest(){
            var log = new NLog.Config.LoggingConfiguration();
            var logconsole = new NLog.Targets.ColoredConsoleTarget("logconsole");
            // Rules for mapping loggers to targets            
            log.AddRule( LogLevel.Debug, LogLevel.Fatal, logconsole);
            // Apply config           
            NLog.LogManager.Configuration = log; 


            kafka = new KafkaConnect();
            var conf = JObject.Parse(@"{
                MessageSendMaxRetries: 100,
                BatchNumMessages:23,
                QueueBufferingMaxKbytes:100,
                QueueBufferingMaxMessages:32,
                MessageTimeoutMs:10,
            }");
            kafka.init(conf);

        }

        [Fact]
        public void Init()
        {
            Assert.Equal(100, kafka._conf._conf.MessageSendMaxRetries);
            Assert.Equal(23, kafka._conf._conf.BatchNumMessages);
            Assert.Equal(100, kafka._conf._conf.QueueBufferingMaxKbytes);
            Assert.Equal(32,kafka._conf._conf.QueueBufferingMaxMessages);
            Assert.Equal(10, kafka._conf._conf.MessageTimeoutMs);
        }
        [Fact]
        public void message(){
            var m = new Message<string,string>{ Value="test bello", Key="hey"};
            kafka.sendMessage("test-topic", m);
            Console.WriteLine("first message sent");
            //System.Threading.Thread.Sleep(10000);
            kafka.sendMessage("test-topic1", m);
            Console.WriteLine("second message sent");

        }
    }
}
