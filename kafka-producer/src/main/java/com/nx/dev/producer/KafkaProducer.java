package com.nx.dev.producer;

import com.nx.dev.dto.Message;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Kafka producer.
 */
@Component
@RequiredArgsConstructor
public class KafkaProducer {

    @Value("${kafka.topic:secret-messages}")
    private String topic;
    private final KafkaTemplate<String, Message> kafkaTemplate;

    @Scheduled(fixedRateString = "${kafka.rate:1000}")
    public void produceMessage() {
        Message message = new Message();
        message.setText("message");

        kafkaTemplate.send(topic, message);
    }
}
