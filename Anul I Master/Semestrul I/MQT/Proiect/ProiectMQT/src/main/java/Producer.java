import avro.mqt.streams.EvenimentAvro;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String CLIENT_ID = "evenimentProducer";
    private static KafkaProducer<String, EvenimentAvro> producer;

    private static String readJsonFromFile (String filePath) {
        try {
            return new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            throw new RuntimeException("Error reading from from JSON file", e);
        }
    }

    public static void main (String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url","http://localhost:8081");

        producer = new KafkaProducer<>(props);

        String jsonContent = readJsonFromFile("src/main/Info.json");

        List<EvenimentAvro> evenimentList;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            evenimentList = objectMapper.readValue(jsonContent, objectMapper.getTypeFactory().constructCollectionType(List.class, EvenimentAvro.class));
        } catch (IOException e) {
            throw new RuntimeException("Error converting JSON to list", e);
        }

        for (EvenimentAvro evenimentAvro : evenimentList) {
            ProducerRecord<String, EvenimentAvro> data = new ProducerRecord<>("topicProiect", evenimentAvro);

            try {
                producer.send(data).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        producer.flush();
        producer.close();
    }
}
