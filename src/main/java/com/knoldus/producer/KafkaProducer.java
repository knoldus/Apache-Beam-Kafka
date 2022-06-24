package com.knoldus.producer;

import com.knoldus.entity.User;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
    public class KafkaProducer {
    private final KafkaTemplate kafkaTemplate;

    public KafkaProducer(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(User[] user) {
        kafkaTemplate.send("knoldus", user);
    }
}

