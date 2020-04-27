package com.nx.dev.consumer;

import com.nx.dev.dto.Message;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer.
 */
@Log4j2
@Component
public class KafkaConsumer {

    @KafkaListener(
            topics = "${kafka.topic:secret-messages}",
            groupId = "${kafka.group:group_id}",
            containerFactory = "concurrentKafkaListenerContainerFactory")
    public void consume(Message message) {
        log.info(message);
    }
}
