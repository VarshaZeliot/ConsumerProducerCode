package demo;

public class MainApplication {

    public static void main(String[] args) {

        ProducerApplication producer = new ProducerApplication();
        ConsumerApplication consumer = new ConsumerApplication();

        Thread producerThread = new Thread(() -> {
            System.out.println("Producer Thread Started");
            try {
                producer.startProducer();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread consumerThread = new Thread(() -> {
            System.out.println("Consumer Thread Started");
            try {
                consumer.startConsumer();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        producerThread.start();
        consumerThread.start();
        producerThread.start();
    }
}