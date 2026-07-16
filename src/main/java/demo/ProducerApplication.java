package demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerApplication {

    public void startProducer() throws Exception {

        String topic = "orders";

        Properties properties = new Properties();

        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);

        int count = 1;

        while (true) {

            String message = "Order-" + count;

            producer.send(new ProducerRecord<>(topic, message));

            System.out.println("Producer Sent : " + message);

            count++;

            Thread.sleep(1000);
        }
    }
}