package com.company;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class Consumer {

    public static void main(String[] args) {
        ConsumerListener c = new ConsumerListener();
        Thread thread = new Thread(c);
        thread.start();
    }

    public static void consumer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("user");
        kafkaConsumer.subscribe(topics);
        try {
            System.out.println("Consumer is Started");
            while (true) {
                ConsumerRecords<String,String > records = kafkaConsumer.poll(1000);
                System.out.println(records);
                if(records.count()==0) {
                    System.out.println("No records");
                }

                for (ConsumerRecord<String,String> record : records) {
                    System.out.printf("Consumer Record:(%s,%s)\n",
                            record.key(), record.value(),
                            record.partition(), record.offset());

                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            kafkaConsumer.close();
        }

    }
}

class ConsumerListener implements Runnable {

    @Override
    public void run() {
        Consumer.consumer();
    }
}