package com.kotlin.kafka.services

import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class MessageConsumer {


    @KafkaListener(topics= ["Topic1"], groupId = "test_id")
    fun consume(message:String) :Unit {
        println(" message received from topic : $message");
    }
}
