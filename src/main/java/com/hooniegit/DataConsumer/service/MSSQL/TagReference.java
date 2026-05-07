package com.hooniegit.DataConsumer.service.MSSQL;

import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 데이터 그룹화를 위한 인덱스 매핑 정보를 보관합니다.
 */
@Component
public class TagReference {

    @Getter @Setter
    private ConcurrentHashMap<Integer, Integer> ids = new ConcurrentHashMap<>();

}
