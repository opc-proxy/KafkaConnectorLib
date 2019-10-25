using System;
using OpcProxyClient; 
using Opc.Ua; 
using OpcProxyCore;
using Newtonsoft.Json.Linq;
using NLog;

namespace opcKafkaConnect
{
    public class KafkaConnect : IOPCconnect
    {
        public void init(JObject config){

        }
        public void OnNotification(object emitter, MonItemNotificationArgs items){

        }
        public void setServiceManager(serviceManager serv){

        }
    }
}
