using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMQ
{
    // Message exchange declare is used at publish and subscriber, also extra queue declare 
    // 
    class PubSubQueue {
        private static ConnectionFactory _factory;
        private static IConnection _connection;
        private static IModel _model;
        private static string _exchangeName = "PubSubExchange";

        internal static void PubSubQueueMain()
        {
            var payment = new Payment() {cardName = "Amex", cardNumber = "12345"};

            CreateQueue();

            ThreadPool.QueueUserWorkItem((obj) =>
            {
                int counter = 0;
                while (counter < 100)
                {
                    SendMessage(payment);
                    Thread.Sleep(100);
                }
            }, null);

            RecieveMessageAtSubscriber();
        }

        private static void CreateQueue()
        {
            _factory = new ConnectionFactory() {HostName = "localhost", UserName = "guest", Password = "guest"};
            _connection = _factory.CreateConnection();
            _model = _connection.CreateModel();//channel

            _model.ExchangeDeclare(_exchangeName, "fanout", true);
        }

        private static void SendMessage(Payment message)
        {
            _model.BasicPublish(_exchangeName, "", null, message.Serialize());
        }


        private static void RecieveMessageAtSubscriber()
        {
            _model.ExchangeDeclare(_exchangeName, "fanout", true);

            var queueName = _model.QueueDeclare().QueueName;

            _model.QueueBind(queueName,_exchangeName,"");

            var consumer = new QueueingBasicConsumer(_model);

            _model.BasicConsume(queueName, true, consumer);

            while (true)
            {
                var ea = consumer.Queue.Dequeue();  
                if(ea==null) return;
                var obj = (Payment) ea.Body.DeSerialize(typeof(Payment));
                Console.WriteLine(obj.cardName + "  ---  " + obj.cardNumber);
            }

        }
    }
}
