//5. Creare ProducerAPI care trimite evenimente sub forma json pe Kafka.

package Tema5;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class JSONProducer {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String CLIENT_ID = "JSONProducer";
    private static KafkaProducer<String, String> producer;

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(properties);

        String JSON = "{\n" +
                "    \"Nume\":\"Cigmaian\",\n" +
                "    \"Prenume\":\"Raluca\",\n" +
                "    \"Varsta\":\"23 de ani\"\n" +
                "}";

        ProducerRecord<String, String> data = new ProducerRecord<>("topicTest", JSON);

        try {
            producer.send(data).get();
        } catch (InterruptedException | ExecutionException e) {
            producer.flush();
        }

        producer.close();
    }
}
