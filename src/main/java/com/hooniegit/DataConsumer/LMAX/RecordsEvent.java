package com.hooniegit.DataConsumer.LMAX;

import jdk.internal.vm.annotation.Contended;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Contended
@Getter @Setter
public class RecordsEvent {

    private ConsumerRecord<String, byte[]> record;

    public void update(ConsumerRecord<String, byte[]> record) {
        this.record = record;
    }

    public void clear() {
        this.record = null;
    }

}
