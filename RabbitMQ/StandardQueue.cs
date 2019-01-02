using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMQ
{
    internal static class  StandardQueue {

    private static ConnectionFactory _factory;
    private static IConnection _connection;
    private static IModel _model;
    private static string _queueName = "StandardQueue";

    internal static void StandardQueueMain()
    {
        Payment payment = new Payment() {cardName = "Amex", cardNumber = "12345"};
        Payment payment1 = new Payment() {cardName = "Amex1", cardNumber = "12345"};
        Payment payment2 = new Payment() {cardName = "Amex2", cardNumber = "12345"};
        Payment payment3 = new Payment() {cardName = "Amex3", cardNumber = "12345"};

        CreateQueue();

        SendMessage(payment);
        SendMessage(payment1);
        SendMessage(payment2);
        SendMessage(payment3);

        RecieveMessage();
    }

    private static void CreateQueue()
    {
        _factory = new ConnectionFactory() {HostName = "localhost", UserName = "guest", Password = "guest"};
        _connection = _factory.CreateConnection();
        _model = _connection.CreateModel();

        _model.QueueDeclare(_queueName, true, false, false, null);
    }

    private static void SendMessage(Payment message)
    {
        _model.BasicPublish("", _queueName, null, message.Serialize());
    }

    private static int GetMessageCount(IModel channel, string queueName)
    {
        var result = channel.QueueDeclare(queueName, true, false, false, null);
        return Convert.ToInt32(result.MessageCount);

    }

    private static void RecieveMessage()
    {
        var consumer = new QueueingBasicConsumer(_model);
        int count = GetMessageCount(_model, _queueName);
        _model.BasicConsume(_queueName, true, consumer);
        int counter = 0;
        while (counter < count)
        {
            var obj = (Payment) consumer.Queue.Dequeue().Body.DeSerialize(typeof(Payment));
            Console.WriteLine(obj.cardName + "  ---  " + obj.cardNumber);
            counter++;
        }

    }

    }
}

