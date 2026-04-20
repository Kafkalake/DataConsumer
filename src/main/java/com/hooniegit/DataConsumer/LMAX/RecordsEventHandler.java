package com.hooniegit.DataConsumer.LMAX;

import com.hooniegit.AeronJavaTools.Common.PrimitiveTagBuffer;
import com.hooniegit.AeronJavaTools.Publisher.DataPublisher;
import com.hooniegit.DataStructure.EDA.Simple.Message;
import com.hooniegit.DataStructure.EDA.Simple.MultiParameter;
import com.hooniegit.Xerializer.Kryo.PoolSerializer;
import com.lmax.disruptor.EventHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.stereotype.Component;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
public class RecordsEventHandler implements EventHandler<RecordsEvent> {

    private final PrimitiveTagBuffer tagBuffer = new PrimitiveTagBuffer(5000);

    private final DataPublisher dataPublisher;

    public RecordsEventHandler(DataPublisher dataPublisher ) {
        this.dataPublisher = dataPublisher;
    }

    @Override
    public void onEvent(RecordsEvent event, long sequence, boolean endOfBatch) {

        try {
            Message<List<MultiParameter>> message = PoolSerializer.deserialize(event.getRecord().value());
//            if (message != null) {
//                String timestamp = (String) message.getHeader().get("local.time");
//
//                for (MultiParameter sp : message.getMessage()) {
//                    int id = sp.getId();
//                    double value = sp.getValue().getValue();
//                    boolean mode = sp.isPmmode();
//
//                    tagBuffer.add(id, value, timestamp);
//
//                    if (endOfBatch || tagBuffer.getSize() >= 5000) {
//                        flushAndSend();
//                    }
//                }
//            } else { System.out.println("Message is null"); }
        } catch (Exception e) {
            System.err.println("Error processing event: " + e.getMessage());
        } finally {
            event.clear();
        }
    }

    private void flushAndSend() {
        if (tagBuffer.getSize() == 0) return;

        // 3. 리팩토링된 원시 타입 기반의 퍼블리싱 메서드 호출
        dataPublisher.publishListDataMessage(
                tagBuffer.getIds(),
                tagBuffer.getValues(),
                tagBuffer.getSize(),
                tagBuffer.getTimestamp()
        );

        // 4. 배열 내부 데이터는 놔두고 인덱스만 0으로 초기화하여 재사용 (GC Zero)
        tagBuffer.clear();
    }

    // 1. 수신된 전체 배치의 용량 확인
//        long totalBytes = getBatchSizeBytes(event.getRecords());
//        double totalMB = toMegaBytes(totalBytes);
//        System.out.printf("Received batch of %d records, total size: %.2f MB%n", event.getRecords().count(), totalMB);

    /**
     * 단일 ConsumerRecord의 정확한 용량(Byte)을 계산합니다.
     * (Key, Value, Headers 모두 포함)
     */
    public static long getRecordSizeBytes(ConsumerRecord<?, ?> record) {
        long totalBytes = 0;

        // 1. Key Size (직렬화된 상태의 크기)
        // 값이 없거나 크기를 알 수 없는 경우 -1을 반환하므로 예외 처리
        if (record.serializedKeySize() > 0) {
            totalBytes += record.serializedKeySize();
        }

        // 2. Value Size (Payload 크기)
        if (record.serializedValueSize() > 0) {
            totalBytes += record.serializedValueSize();
        }

        // 3. Header Size (네트워크 전송 시 무시할 수 없는 용량)
        for (Header header : record.headers()) {
            // 헤더 키의 문자열 바이트 길이
            totalBytes += header.key().getBytes(StandardCharsets.UTF_8).length;
            // 헤더 값의 바이트 길이
            if (header.value() != null) {
                totalBytes += header.value().length;
            }
        }

        // 카프카 내부 오프셋, 타임스탬프 등 메타데이터를 위한 약간의 오버헤드(약 60~100바이트)가
        // 존재하지만, 메모리 병목의 주원인은 아니므로 통상적으로 위 3개만 합산합니다.
        return totalBytes;
    }

    /**
     * 1회 poll()로 수신한 ConsumerRecords 전체 배치의 용량(Byte)을 계산합니다.
     */
    public static long getBatchSizeBytes(ConsumerRecords<?, ?> records) {
        long totalBatchBytes = 0;

        for (ConsumerRecord<?, ?> record : records) {
            totalBatchBytes += getRecordSizeBytes(record);
        }

        return totalBatchBytes;
    }

    /**
     * 로깅을 위해 Byte를 MB(메가바이트)로 변환합니다.
     */
    public static double toMegaBytes(long bytes) {
        return (double) bytes / (1024 * 1024);
    }

}
