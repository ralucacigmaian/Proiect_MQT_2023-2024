//6. Creare ConsumerAPI Kafka ptr Producer-ul de mai sus.

package Tema6V2;

import Tema4.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

public class PersoanaConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(PersoanaConsumer.class);

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String OFFSET_RESET = "earliest";
    private static final String GROUP_ID = "persoanaConsumer";

    KafkaConsumer<String, String> kafkaConsumer;

    public PersoanaConsumer (Properties consumerPropsMap) {
        kafkaConsumer = new KafkaConsumer<String, String>(consumerPropsMap);
    }

    public void pollKafka (String kafkaTopicName) {
        kafkaConsumer.subscribe(Collections.singleton(kafkaTopicName));

        Duration pollingTime = Duration.of(2, ChronoUnit.SECONDS);

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(pollingTime);

            records.forEach(record -> {
                LOG.info("For topic {} the value is {}", kafkaTopicName, record.value());
            });
        }
    }

    public static void main (String[] args) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        PersoanaConsumer consumer = new PersoanaConsumer(properties);
        consumer.pollKafka("topicTest");
    }
}
