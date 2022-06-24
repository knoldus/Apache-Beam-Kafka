package com.knoldus.controller;

import com.knoldus.consumer.KafkaConsumer;
import com.knoldus.entity.User;
import com.knoldus.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/kafka")
public class KafkaProducerController {
    private final KafkaProducer kafkaProducer;

    @Autowired
    private KafkaConsumer kafkaConsumer;
    public KafkaProducerController(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostMapping("/producesMessage")
    public String sendMessage(@RequestBody User user[]) {
        kafkaProducer.sendMessage(user);
        return "Message Produced Successfully";
    }

    @GetMapping("/user")
    public String getUser(String messages) {
        List<String> list= kafkaConsumer.consumeMessage(messages);
        System.out.println("Consumed Messages: " +list);
        return "Message consumed successfully";
    }
}
