//5. Creare ProducerAPI care trimite evenimente sub forma de obiecte definite (clase Java, C#, etc) in API pe Kafka.

package Tema5V2;

import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class PersoanaProducer {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String CLIENT_ID = "persoanaProducer";
    private static KafkaProducer<String, Persoana> producer;
    public static void main (String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());

        producer = new KafkaProducer<>(properties);

        Persoana persoana = new Persoana("Cigmaian", "Raluca", 23, "Timisoara");

        ProducerRecord<String, Persoana> data = new ProducerRecord<>("topicTest", persoana);

        try {
            producer.send(data).get();
        } catch (InterruptedException | ExecutionException e) {
            producer.flush();
        }

        producer.close();
    }
}
