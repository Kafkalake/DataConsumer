package com.hooniegit.DataConsumer.LMAX;

import com.lmax.disruptor.EventHandler;

public class ClearEventHandler implements EventHandler<RecordsEvent> {
    @Override
    public void onEvent(RecordsEvent event, long sequence, boolean endOfBatch) {
        event.clear(); // 링 버퍼 한 칸의 처리가 완전히 끝났을 때 비워줌
    }
}

