package com.hooniegit.DataConsumer.Kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class KafkaConsumerService implements ConsumerSeekAware {

    /**
     * On Partitions Assigned Callback
     * - Seek to End of Each Partition
     */
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekAware.ConsumerSeekCallback callback) {
        assignments.keySet().forEach(partition -> {
            callback.seekToEnd("WAT", partition.partition());
        });
    }

    private void task(ConsumerRecord<String, byte[]> record) {

    }

    @KafkaListener(topics = "WAT", containerFactory = "kafkaListenerContainerFactory")
    public void consume(ConsumerRecord<String, byte[]> record) {
        task(record);
    }

}
