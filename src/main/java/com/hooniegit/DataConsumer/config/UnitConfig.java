package com.hooniegit.DataConsumer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * ini 파일에 정의된 unit 정보를 기반으로 사업소를 설정합니다.
 * @position  기타 Configuration 클래스
 * @author    @hooniegit
 */
@Component
@ConfigurationProperties(prefix = "opc.unit")
@Getter @Setter
public class UnitConfig {

    private String UNIT;

}
