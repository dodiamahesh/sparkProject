package com.demo.etl.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Creates ONE SparkSession for the whole application.
 * A SparkSession is thread-safe, so every pipeline can share it.
 */
@Configuration
public class SparkConfig {

    private static final Logger log = LoggerFactory.getLogger(SparkConfig.class);

    @Bean(destroyMethod = "stop")
    public SparkSession sparkSession(EtlProperties properties) {
        EtlProperties.Spark sparkProperties = properties.spark();

        SparkConf conf = new SparkConf()
                .setAppName(sparkProperties.appName())
                .setMaster(sparkProperties.master());

        if (sparkProperties.config() != null) {
            sparkProperties.config().forEach(conf::set);
        }

        log.info("Starting SparkSession (master={}, app={})", sparkProperties.master(), sparkProperties.appName());
        return SparkSession.builder().config(conf).getOrCreate();
    }
}
