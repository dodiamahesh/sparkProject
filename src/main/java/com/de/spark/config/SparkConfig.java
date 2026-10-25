package com.de.spark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds the SparkSession from "spark.*" properties.
 * local[*] by default; switch to a cluster just by changing properties (no code change).
 */
@Configuration
@ConfigurationProperties(prefix = "spark")
public class SparkConfig {

    private String master = "local[*]";
    private String appName = "weather-etl";
    private Map<String, String> conf = new HashMap<>();

    @Bean(destroyMethod = "stop")
    public SparkSession sparkSession() {
        SparkConf sparkConf = new SparkConf().setMaster(master).setAppName(appName);
        conf.forEach(sparkConf::set);
        return SparkSession.builder().config(sparkConf).getOrCreate();
    }

    public void setMaster(String master) { this.master = master; }
    public void setAppName(String appName) { this.appName = appName; }
    public Map<String, String> getConf() { return conf; }
}
