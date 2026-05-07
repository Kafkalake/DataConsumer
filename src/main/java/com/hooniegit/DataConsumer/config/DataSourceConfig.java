package com.hooniegit.DataConsumer.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * ini 파일에 정의된 MSSQL 연결 정보를 기반으로 DataSource와 JdbcTemplate을 생성합니다.
 * @position  MSSQL Configuration 클래스
 * @author    @hooniegit
 */
@Configuration
public class DataSourceConfig {

    @Bean(name = "tagDataSource")
    @ConfigurationProperties(prefix = "datasource.tag")
    public DataSource tagDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "tagJdbcTemplate")
    public JdbcTemplate tagJdbcTemplate(@Qualifier("tagDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }

}

