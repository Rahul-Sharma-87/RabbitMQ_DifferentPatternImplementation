using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQ
{
    [Serializable]
    internal class Payment {
        public string cardName { get; set; } 
        public string cardNumber{ get; set; }

        
    }

    internal static class PaymentExtn {
        public static byte[] Serialize(this Payment payment)
        {
            var json = JsonConvert.SerializeObject(payment);
            return Encoding.ASCII.GetBytes(json);
        }

        public static byte[] Serialize(this PaymentDetail paymentDetail)
        {
            var json = JsonConvert.SerializeObject(paymentDetail);
            return Encoding.ASCII.GetBytes(json);
        }

        public static object DeSerialize(this Byte[] data,Type type)
        {
            var jsonString = Encoding.Default.GetString(data);
            return JsonConvert.DeserializeObject(jsonString, type);
        }
    }

    
    [Serializable]
    internal class PaymentDetail {
        public string PaymentType { get; set; } 
        public string PaymentAmount{ get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            //StandardQueue();
            //WorkerQueue.WorkerQueueMain();
            //PubSubQueue.PubSubQueueMain();
            DirectQueue.SendDirectQueue();
        }
    }
}
