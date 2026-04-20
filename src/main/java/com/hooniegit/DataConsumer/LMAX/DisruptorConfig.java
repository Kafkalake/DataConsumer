package com.hooniegit.DataConsumer.LMAX;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
public class DisruptorConfig {

    // 처리량에 따라 설정 (반드시 2의 거듭제곱이어야 함)
    // 초당 18만 건이므로, 2^19 (524,288) 또는 2^20 (1,048,576) 정도가 안정적입니다.
    private static final int RING_BUFFER_SIZE = 1024 * 64;

    @Bean
    public Disruptor<RecordsEvent> recordsEventDisruptor(RecordsEventHandler handler) {
        // 1. Event Factory: 링 버퍼에 사전 할당할 객체 생성 규칙
        RecordsEventFactory factory = new RecordsEventFactory();

        // 2. Thread Factory: Worker 스레드 생성 (이름 부여로 모니터링 용이)
        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger index = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "Disruptor-Worker-" + index.getAndIncrement());
            }
        };

        Disruptor<RecordsEvent> disruptor = new Disruptor<>(
                factory,
                RING_BUFFER_SIZE,
                threadFactory,
                ProducerType.MULTI, // [중요] 64개의 Kafka 스레드가 동시에 Publish 하므로 MULTI 필수
                new YieldingWaitStrategy()
        );

        // 전송 로직이 병목을 일으킬 수 있다면 handleEventsWithWorkerPool()을 통해 여러 Worker가 분담하게 할 수도 있습니다.
        disruptor.handleEventsWith(handler).then(new ClearEventHandler());
        disruptor.start();

        return disruptor;
    }

    @Bean
    public RingBuffer<RecordsEvent> sensorEventRingBuffer(Disruptor<RecordsEvent> disruptor) {
        return disruptor.getRingBuffer();
    }

}