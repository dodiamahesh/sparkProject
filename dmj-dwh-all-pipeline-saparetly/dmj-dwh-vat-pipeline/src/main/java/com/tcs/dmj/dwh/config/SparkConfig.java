package com.tcs.dmj.dwh.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** One SparkSession for the whole application (thread-safe, shared by every pipeline). */
@Configuration
public class SparkConfig {

    private static final Logger log = LoggerFactory.getLogger(SparkConfig.class);

    @Bean(destroyMethod = "stop")
    public SparkSession sparkSession(ApplicationProperties properties) {
        ApplicationProperties.Spark settings = properties.spark();
        SparkConf conf = new SparkConf()
                .setAppName(settings.appName())
                .setMaster(settings.master());
        if (settings.config() != null) {
            settings.config().forEach(conf::set);
        }
        log.info("Starting SparkSession (master={}, app={})", settings.master(), settings.appName());
        return SparkSession.builder().config(conf).getOrCreate();
    }
}
