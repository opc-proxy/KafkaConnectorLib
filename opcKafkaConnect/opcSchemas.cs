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

namespace opcKafkaConnect{
    class opcSchemas{
        public RecordSchema stringType;
        public RecordSchema doubleType;
        public RecordSchema intType;
        public RecordSchema booleanType;
        public RecordSchema floatType;
        public RecordSchema longType;

        opcSchemas(){
            stringType  = (RecordSchema)RecordSchema.Parse(buildSchema("string"));
            doubleType  = (RecordSchema)RecordSchema.Parse(buildSchema("double"));
            intType     = (RecordSchema)RecordSchema.Parse(buildSchema("int"));
            floatType   = (RecordSchema)RecordSchema.Parse(buildSchema("float"));
            booleanType = (RecordSchema)RecordSchema.Parse(buildSchema("boolean"));
            longType    = (RecordSchema)RecordSchema.Parse(buildSchema("long"));
        }

        string buildSchema(string type){
            return "{type: 'record',name:'"+type+"Type',fields:[{name:'value', type:'"+type+"'}]}";
        }

        public RecordSchema GetSchema(Type t){
            TypeCode code = Type.GetTypeCode(t);

            switch(code){
                case TypeCode.Int16 :
                    return intType;
                case TypeCode.Int32 :
                    return intType;
                case TypeCode.Int64 :
                    return longType;
                case TypeCode.Boolean :
                    return booleanType;
                case TypeCode.Single:
                    return floatType;
                case TypeCode.Double:
                    return doubleType;
                case TypeCode.String:
                    return stringType;
                default:
                    return null;
            }
        }

        /// <summary>
        /// Helper function to get the right serialization type for an object according to Avro
        /// serialization conventions. The only problem is with int16 actually.
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public Type getAvroType(Type t){
            TypeCode code = Type.GetTypeCode(t);
            switch(code){
                case TypeCode.Int16 :
                    return typeof(System.Int32);
                case TypeCode.Int32 :
                    return typeof(System.Int32);
                case TypeCode.Int64 :
                    return typeof(System.Int64);
                case TypeCode.Boolean :
                    return typeof(System.Boolean);
                case TypeCode.Single:
                    return typeof(System.Single);
                case TypeCode.Double:
                    return typeof(System.Double);
                case TypeCode.String:
                    return typeof(System.String);
                default:
                    return null;
            }
        }
    }
}