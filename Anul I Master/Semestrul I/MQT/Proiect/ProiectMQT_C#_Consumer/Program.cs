using Confluent.Kafka;
using System.Text.Json;

namespace ProiectMQT_C_Consumer
{
    public class Consumer
    {
        static void Main(string[] args)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "my-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
            {
                consumer.Subscribe("topicProiect");

                while (true)
                {
                    var consumerResult = consumer.Consume();
                    var eveniment = JsonSerializer.Deserialize<Eveniment>(consumerResult.Message.Value);
                    Console.WriteLine($"Received Eveniment: {eveniment.name}, {eveniment.address}, {eveniment.weight}, {eveniment.tall}, {eveniment.phone}, {eveniment.email}, {eveniment.advices}");
                }
            }
        }
    }
}