package com.knoldus.consumer;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class KafkaConsumer {
    List<String> list = new ArrayList<>();
    @KafkaListener(topics = "knoldus" ,groupId = "knoldus pvt ltd")
    public List<String> consumeMessage(String message){

        list.add(message);
        return list;
    }
}
