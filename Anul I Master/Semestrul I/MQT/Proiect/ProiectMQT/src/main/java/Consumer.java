import avro.mqt.streams.EvenimentAvro;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String OFFSET_RESET = "earliest";
    private static final String GROUP_ID = "evenimentConsumer";

    KafkaConsumer<String, EvenimentAvro> kafkaConsumer;

    public Consumer (Properties consumerPropsMap) {
        kafkaConsumer = new KafkaConsumer<String, EvenimentAvro>(consumerPropsMap);
    }

    public void pollKafka (String kafkaTopicName) {
        kafkaConsumer.subscribe(Collections.singleton(kafkaTopicName));

        Duration pollingTime = Duration.of(2, ChronoUnit.SECONDS);

        while (true) {
            ConsumerRecords<String, EvenimentAvro> records = kafkaConsumer.poll(pollingTime);

            records.forEach(record -> {
                LOG.info("For topic {} the value is {}", kafkaTopicName, record.value());
            });
        }
    }

    public static void main (String[] args) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("schema.registry.url","http://localhost:8081");

        Consumer consumer = new Consumer(props);
        consumer.pollKafka("topicProiect");
    }
}
