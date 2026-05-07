package com.hooniegit.DataConsumer.service.MSSQL;

import com.hooniegit.DataConsumer.config.UnitConfig;
import jakarta.annotation.PostConstruct;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 데이터베이스에서 태그 정보를 주기적으로 갱신하는 서비스 클래스입니다.
 */
@Service
public class TagService {

    // 클래스 내부 속성
    private final JdbcTemplate TEMPLATE;
    private final TagReference REFERENCE;
    private final UnitConfig UNIT;
    private final Logger LOGGER = LogManager.getLogger(TagService.class);
    private String endpoint;

    @Autowired
    public TagService(JdbcTemplate TEMPLATE,
                      TagReference REFERENCE,
                      UnitConfig UNIT) {
        this.TEMPLATE = TEMPLATE;
        this.REFERENCE = REFERENCE;
        this.UNIT = UNIT;
    }

    @PostConstruct
    public void initialTask() {
        updateEndpoint();
        updateIds();
    }

    @Scheduled(cron = "${datasource.tag.schedule}")
    public void periodicalTask() {
        try {
            updateEndpoint();
            updateIds();
        } catch (Exception ex) {
            this.LOGGER.warn(ex.toString());
        }
    }

    private void updateEndpoint() {
        String unitName = this.UNIT.getUNIT();

        String getSql = String.format("""
            SELECT endpoint
            FROM ctc_kafka.dbo.kf_site
            WHERE site = N'%s';
        """, unitName);

        System.out.println(getSql);

        this.TEMPLATE.query(getSql, rs -> {
            this.endpoint = rs.getString("endpoint");
            System.out.println(this.endpoint);
        });
    }

    /**
     * 쿼리 결과를 참조하여 REFERENCE 데이터를 갱신합니다.
     */
    private void updateIds() {
        String getSql = String.format("""
            SELECT id, opcid
            FROM ctc_kafka.dbo.kf_tag%s;
        """, endpoint);

        System.out.println(getSql);

        REFERENCE.setIds(this.TEMPLATE.query(getSql, rs -> {
            ConcurrentHashMap<Integer, Integer> resultMap = new ConcurrentHashMap<>();
            while (rs.next()) {
                resultMap.put(rs.getInt("id"), rs.getInt("opcid"));
            }
            return resultMap;
        }));
    }

}
