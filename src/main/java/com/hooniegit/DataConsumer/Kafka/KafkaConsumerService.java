package com.hooniegit.DataConsumer.Kafka;

import com.hooniegit.DataConsumer.LMAX.RecordsEvent;
import com.lmax.disruptor.RingBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Service;
import java.util.Map;

@Service
public class KafkaConsumerService implements ConsumerSeekAware {

    private final RingBuffer<RecordsEvent> ringBuffer;

    public KafkaConsumerService(RingBuffer<RecordsEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    /**
     * 파티션 재할당 시점에 오프셋 정보를 초기화합니다.
     */
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekAware.ConsumerSeekCallback callback) {
        assignments.keySet().forEach(partition -> callback.seekToEnd("WAT", partition.partition()));
    }

    private void task(ConsumerRecords<String, byte[]> records) {

        for (ConsumerRecord<String, byte[]> record: records) {
            long sequence = ringBuffer.next();
            RecordsEvent event = ringBuffer.get(sequence);
            event.setRecord(record);
            ringBuffer.publish(sequence);
        }


    }

    /**
     * Kafka 리스너에서 데이터 수신 시점에 수행할 작업을 정의합니다.
     * @param records 리스너에서 수신한 Kafka Consumer Record 묶음
     */
    @KafkaListener(topics = "WAT", containerFactory = "kafkaListenerContainerFactory")
    public void consume(ConsumerRecords<String, byte[]> records) {
        task(records);
    }

}
