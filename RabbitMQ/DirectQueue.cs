using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Framing.Impl;

namespace RabbitMQ
{
    /// <summary>
    /// DirectQueue uses same as sub sub but along with the route key so there is dedicated queue for specifc message types
    /// </summary>
    static class DirectQueue
    {
        private static ConnectionFactory _factory;
        private static IConnection _connection;
        private static IModel _channel;
        private static string _exchangeName = "DirectExchange";
        private static string _paymentQueueName = "Payment";
        private static string _paymentDetailQueueName= "PaymentDetail";

        private static string _paymentRouteKey = "CardPayment";
        private static string _paymentDetailRouteKey = "CardPaymentDetail";

        internal static void SendDirectQueue() {
            CreateAndSend();
            ThreadPool.QueueUserWorkItem((obj) => { RecievePaymentMessage(); }, null);
            ThreadPool.QueueUserWorkItem((obj) => { RecievePaymentDetailMessage(); }, null);
        }

        private static void CreateAndSend()
        {
            _factory = new ConnectionFactory() {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };

            _connection = _factory.CreateConnection();

            _channel = _connection.CreateModel();

            _channel.ExchangeDeclare(_exchangeName,"direct");

            //
            _channel.QueueDeclare(_paymentQueueName, true, false, false, null);

            _channel.QueueDeclare(_paymentDetailQueueName, true, false, false, null);

            //
            _channel.QueueBind(_paymentQueueName,_exchangeName,_paymentRouteKey);

            _channel.QueueBind(_paymentDetailQueueName,_exchangeName,_paymentDetailRouteKey);

            ThreadPool.QueueUserWorkItem((obj) =>
            {
                int counter = 0;
                while (counter < 100) {
                    SendMessage();
                    Thread.Sleep(100);
                    counter++;
                }
            },null);


        }

        private static void SendMessage()
        {
            var payment = new Payment() {cardName = "Amex", cardNumber = "12345"};

            var paymentDetail = new PaymentDetail() {PaymentType = "Card", PaymentAmount = "10000"};

            _channel.BasicPublish(
                _exchangeName,
                _paymentRouteKey,
                false,
                null, 
                payment.Serialize()
                );

            _channel.BasicPublish(
                _exchangeName,
                _paymentDetailRouteKey,
                false,
                null, 
                paymentDetail.Serialize()
                );
        }

        private static void RecievePaymentMessage()
        {
            IConnectionFactory factory = new ConnectionFactory(){HostName = "localhost", UserName = "guest", Password = "guest"};
            using (var connection = factory.CreateConnection()) {
                using (var model = connection.CreateModel()) {
                    model.ExchangeDeclare(_exchangeName,"direct");
                    model.QueueDeclare(_paymentQueueName, true, false, false, null);
                    model.QueueBind(_paymentQueueName, _exchangeName, _paymentRouteKey);
                    model.BasicQos(0,1,false);
                    var consumer = new QueueingBasicConsumer(model);
                    model.BasicConsume(_paymentQueueName, false, consumer);
                    while (true) {
                        var ea = consumer.Queue.Dequeue();
                        model.BasicAck(ea.DeliveryTag,false);
                        Payment payment = (Payment) ea.Body.DeSerialize(typeof(Payment));
                        Console.WriteLine(payment.cardNumber +"  ---   "+payment.cardName);
                    }
                }
            }


        }

        private static void RecievePaymentDetailMessage() {
            IConnectionFactory factory = new ConnectionFactory(){HostName = "localhost", UserName = "guest", Password = "guest"};
            using (var connection = factory.CreateConnection()) {
                using (var model = connection.CreateModel()) {
                    model.ExchangeDeclare(_exchangeName,"direct");
                    model.QueueDeclare(_paymentDetailRouteKey, true, false, false, null);
                    model.QueueBind(_paymentDetailQueueName, _exchangeName, _paymentDetailRouteKey);
                    model.BasicQos(0,1,false);
                    var consumer = new QueueingBasicConsumer(model);
                    model.BasicConsume(_paymentDetailQueueName, false, consumer);
                    while (true) {
                        var ea = consumer.Queue.Dequeue();
                        model.BasicAck(ea.DeliveryTag,false);
                        PaymentDetail paymentDetail = (PaymentDetail) ea.Body.DeSerialize(typeof(PaymentDetail));
                        Console.WriteLine(paymentDetail.PaymentType +"  ---   "+paymentDetail.PaymentAmount);
                    }
                }
            }
                
        }

    }
}
