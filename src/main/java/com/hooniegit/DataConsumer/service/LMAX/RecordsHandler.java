package com.hooniegit.DataConsumer.service.LMAX;

import com.hooniegit.AeronJavaTools.Common.TagData;
import com.hooniegit.DataConsumer.service.MSSQL.TagReference;
import com.hooniegit.DataStructure.EDA.Simple.Message;
import com.hooniegit.DataStructure.EDA.Simple.MultiParameter;
import com.hooniegit.Xerializer.Kryo.PoolSerializer;
//import com.hooniegit.Xerializer.Kryo.ThreadLocalSerializer;
import com.hooniegit.Xtream.Tools.Event;
import com.hooniegit.Xtream.Tools.Handler;
import com.hooniegit.AeronJavaTools.Publisher.DataPublisher;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * 예제 커스텀 핸들러 클래스입니다.
 *
 * 시스템 구성을 위해서 이하의 작업을 수행해야 합니다.
 * - `Sample` 클래스를 이벤트가 사용할 데이터 클래스로 변경
 * - `process` 메서드에 이벤트 처리기가 수행할 작업을 구성
 */

@Getter
public class RecordsHandler extends Handler<byte[]> {

    private final DataPublisher dataPublisher;

    private final TagReference tagReference;

    public RecordsHandler(DataPublisher dataPublisher, TagReference tagReference) {
        this.dataPublisher = dataPublisher;
        this.tagReference = tagReference;
    }

    /**
     * On Event
     */
    @Override
    protected void process(Event<byte[]> event) {

        List<TagData<Double>> list = new ArrayList<>();

        try {
            Message<List<MultiParameter>> message = PoolSerializer.deserialize(event.getData());
            String timestamp = (String) message.getHeader().get("local.time");

            for (MultiParameter m : message.getMessage()) {
                int id = m.getId();

                if (tagReference.getIds().containsKey(id)) {
                    int tag_index = tagReference.getIds().get(id);
                    list.add(new TagData(tag_index, m.getValue().getValue()));
                }
            }

//            dataPublisher.publishListDataMessage(list, timestamp);
            System.out.println(timestamp); // TEST

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
        }

    }

}
