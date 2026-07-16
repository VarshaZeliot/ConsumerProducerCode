package demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerApplication {

    public void startConsumer() {

        String topic = "orders";

        Properties properties = new Properties();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "demo-group");

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