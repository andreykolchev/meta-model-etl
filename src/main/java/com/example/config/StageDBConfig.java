//package com.example.config;
//
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.jdbc.core.JdbcTemplate;
//import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
//import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
//
//import javax.sql.DataSource;
//
//@Configuration
//public class StageDBConfig {
//
//    @Bean("sourceDatasource")
//    public DataSource sourceDatasource() {
//        return new EmbeddedDatabaseBuilder()
//                .setType(EmbeddedDatabaseType.H2)
//                .setName("source")
//                .addScript("classpath:data/source.sql").build();
//    }
//
//    @Bean("sourceDataJdbcTemplate")
//    public JdbcTemplate sourceDataJdbcTemplate() {
//        return new JdbcTemplate(sourceDatasource());
//    }
//
//}
