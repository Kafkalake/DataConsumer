package com.hooniegit.DataConsumer.LMAX;

import com.lmax.disruptor.EventFactory;

public class RecordsEventFactory implements EventFactory<RecordsEvent> {
    @Override
    public RecordsEvent newInstance() {
        RecordsEvent event = new RecordsEvent();
        return new RecordsEvent();
    }
}
