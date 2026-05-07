package com.hooniegit.DataConsumer.service.Aeron;

import com.hooniegit.AeronJavaTools.Publisher.DataPublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 *
 */
@Configuration
public class AeronPublisherConfig {

//    @Bean(initMethod = "connect", destroyMethod = "disconnect")
    @Bean(destroyMethod = "disconnect")
    public DataPublisher aeronDataPublisher(
            @Value("${aeron.server.location:aeron-sbe-ipc}") String location,
            @Value("${aeron.server.stream:10}") int streamId
    ) {
        // 객체 생성 후 Spring이 자동으로 connect()를 호출하고,
        // 애플리케이션 종료 시 disconnect()를 호출하여 자원을 안전하게 해제합니다.
        return new DataPublisher(location, streamId);
    }

}
