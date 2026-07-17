package demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerApplication {

    public void startConsumer() {

        Properties config = new Properties();

        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {

            if (input == null) {
                throw new RuntimeException("config.properties file not found");
            }

            config.load(input);

        } catch (IOException e) {
            throw new RuntimeException("Failed to load config.properties", e);
        }

        String bootstrapServers = config.getProperty("BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = config.getProperty("TOPIC_NAME", "orders");
        String consumerGroup = config.getProperty("CONSUMER_GROUP", "demo-group");

        String saslProtocol = config.getProperty("SECURITY_PROTOCOL");
        String saslMechanism = config.getProperty("SASL_MECHANISM");
        String saslUsername = config.getProperty("SASL_USERNAME");
        String saslPassword = config.getProperty("SASL_PASSWORD");
        String saslTlsVersion = config.getProperty("SSL_ENABLED_PROTOCOLS");

        Properties properties = new Properties();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "demo-group-1");

        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(topic));

        System.out.println("Consumer subscribed to topic : " + topic);

        while (true) {

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            System.out.println("Records received = " + records.count());

            for (ConsumerRecord<String, String> record : records) {

                System.out.println("Consumer Received : " + record.value());

            }
        }
    }
}