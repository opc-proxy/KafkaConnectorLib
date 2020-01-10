using System;
using Avro;

namespace opcKafkaConnect
{
    public class opcSchemas
    {
        public RecordSchema stringType;
        public RecordSchema doubleType;
        public RecordSchema intType;
        public RecordSchema booleanType;
        public RecordSchema floatType;
        public RecordSchema longType;
        public RecordSchema rpcRequest;
        public RecordSchema rpcResponse;
        public RecordSchema rpcError;

        public opcSchemas()
        {
            stringType = (RecordSchema)RecordSchema.Parse(buildSchema("string"));
            doubleType = (RecordSchema)RecordSchema.Parse(buildSchema("double"));
            intType = (RecordSchema)RecordSchema.Parse(buildSchema("int"));
            floatType = (RecordSchema)RecordSchema.Parse(buildSchema("float"));
            booleanType = (RecordSchema)RecordSchema.Parse(buildSchema("boolean"));
            longType = (RecordSchema)RecordSchema.Parse(buildSchema("long"));

            rpcRequest = (RecordSchema)RecordSchema.Parse(@"{
                    'name': 'JSON_RPC_2_0_Request',
                    'type': 'record',
                    'fields': [
                        {
                            'name': 'method',
                            'type': 'string'
                        },
                        {
                            'name': 'params',
                            'type':['null',{'type':'array', 'items': 'string'}],
                            'default': null
                        },
                        {
                            'name': 'id',
                            'type': ['null','long'],
                            'default' : null
                        }
                    ]
                }");

            rpcResponse = (RecordSchema)RecordSchema.Parse(@"
                                    {
                        'name': 'JSON_RPC_2_0_Response',
                        'type': 'record',
                        'fields': [
                            {
                                'name': 'result',
                                'type': ['null','string'],
                                'default':null
                            },
                            {
                                'name': 'error',
                                'type':[
                                    'null',
                                    {
                                        'type':'record', 
                                        'name':'error', 
                                        'fields': [
                                            {'name':'code', 'type':'int'},
                                            {'name':'message', 'type':'string'}
                                        ]
                                    }
                                ],
                                'default': null
                            },
                            {
                                'name': 'id',
                                'type': 'long'
                            }
                        ]
                    }");

            rpcError = (RecordSchema)RecordSchema.Parse(@"
                {
                    'type':'record', 
                    'name':'error', 
                    'fields': [
                        {'name':'code', 'type':'int'},
                        {'name':'message', 'type':'string'}
                    ]
                }");
        }

        string buildSchema(string type)
        {
            return "{type: 'record',name:'" + type + "Type',fields:[{name:'value', type:'" + type + "'}]}";
        }

        public RecordSchema GetSchema(Type t)
        {
            TypeCode code = Type.GetTypeCode(t);

            switch (code)
            {
                case TypeCode.Int16:
                    return intType;
                case TypeCode.Int32:
                    return intType;
                case TypeCode.Int64:
                    return longType;
                case TypeCode.Boolean:
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
        public static Type getAvroSerializationType(Type t)
        {
            TypeCode code = Type.GetTypeCode(t);
            switch (code)
            {
                case TypeCode.Int16:
                    return typeof(System.Int32);
                case TypeCode.Int32:
                    return typeof(System.Int32);
                case TypeCode.Int64:
                    return typeof(System.Int64);
                case TypeCode.Boolean:
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


        /// <summary>
        /// Helper function to get the Avro type for an object according to Avro
        /// serialization names.
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public static string getAvroType(Type t)
        {
            TypeCode code = Type.GetTypeCode(t);
            switch (code)
            {
                case TypeCode.Int16:
                    return "int";
                case TypeCode.Int32:
                    return "int";
                case TypeCode.Int64:
                    return "long";
                case TypeCode.Boolean:
                    return "boolean";
                case TypeCode.Single:
                    return "float";
                case TypeCode.Double:
                    return "double";
                case TypeCode.String:
                    return "string";
                default:
                    return null;
            }
        }
    }
}