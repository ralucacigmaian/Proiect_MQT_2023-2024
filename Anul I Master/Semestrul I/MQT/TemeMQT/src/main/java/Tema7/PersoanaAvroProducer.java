//7. Creare ProducerAPI si publicare mesaje folosind schema for Kafka.

package Tema7;

import Tema5V2.Persoana;
import avro.PersoanaAvro;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class PersoanaAvroProducer {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String CLIENT_ID = "avroPersoanaProducer";
    private static KafkaProducer<String, PersoanaAvro> producer;
    public static void main (String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url","http://localhost:8081");

        producer = new KafkaProducer<>(properties);

        PersoanaAvro persoanaAvro = new PersoanaAvro("Cigmaian", "Raluca", 23, "Timisoara");

        ProducerRecord<String, PersoanaAvro> data = new ProducerRecord<>("topicTest", persoanaAvro);

        try {
            producer.send(data).get();
        } catch (InterruptedException | ExecutionException e) {
            producer.flush();
        }

        producer.close();
    }
}
