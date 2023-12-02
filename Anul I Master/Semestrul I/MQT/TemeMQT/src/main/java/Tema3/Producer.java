//3. Creare ProducerAPI care trimite evenimente sub forma de siruri de cacactere pe Kafka.

package Tema3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String CLIENT_ID = "stringProducer";
    private static KafkaProducer<String, String> producer;

    public static void main (String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> data1 = new ProducerRecord<>("topicTest", "Test Value 1");
        ProducerRecord<String, String> data2 = new ProducerRecord<>("topicTest", "Test Value 2");

        try {
            producer.send(data1).get();
            producer.send(data2).get();
        } catch (InterruptedException | ExecutionException e) {
            producer.flush();
        }

        producer.close();
    }
}
