using System;
using Confluent.Kafka;
using System.Text.Json;

namespace ProiectMQT_C_
{
    public class Producer
    {
        static Eveniment[] LoadDataFromJson(string filePath)
        {
            try
            {
                using (StreamReader r = new StreamReader(filePath))
                {
                    string json = r.ReadToEnd();
                    return JsonSerializer.Deserialize<Eveniment[]>(json);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error loading data from JSON file: {e.Message}");
                return new Eveniment[0];
            }
        }

        static void Main(string[] args)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

            while (true)
            {
                var evenimentData = LoadDataFromJson("Info.json");

                foreach (var eveniment in evenimentData)
                {
                    Console.WriteLine($"Sending Eveniment for {eveniment.name}");
                    producer.Produce("topicProiect", new Message<Null, string> { Value = JsonSerializer.Serialize(eveniment) });
                    Thread.Sleep(1000);
                }
            }
        }
    }
}

