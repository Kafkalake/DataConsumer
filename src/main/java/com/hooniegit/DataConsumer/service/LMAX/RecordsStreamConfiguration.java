package com.hooniegit.DataConsumer.service.LMAX;

import java.util.List;

import com.hooniegit.AeronJavaTools.Publisher.DataPublisher;
import com.hooniegit.DataConsumer.service.MSSQL.TagReference;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.hooniegit.Xtream.Tools.Handler;
import com.hooniegit.Xtream.Tools.StreamAutoConfiguration;


@Configuration
@Import(StreamAutoConfiguration.class)
public class RecordsStreamConfiguration {

    @Bean
    public List<Handler<byte[]>> handlers(DataPublisher dataPublisher, TagReference tagReference) {
        return List.of(new RecordsHandler(dataPublisher, tagReference));
    }

    @Bean
    public StreamAutoConfiguration<byte[]> streamAutoConfiguration() {
        return new StreamAutoConfiguration<>();
    }

}
