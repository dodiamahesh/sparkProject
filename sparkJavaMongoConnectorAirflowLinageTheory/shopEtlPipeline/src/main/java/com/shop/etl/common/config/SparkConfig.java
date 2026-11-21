package com.shop.etl.common.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.TimeZone;

/**
 * Aakhi application mate EK j SparkSession banave che.
 *
 * Kem ek j? SparkSession banavvu moghu che (JVM ma Spark context, thread pools, memory).
 * SparkSession thread-safe che, etle API, scheduler ane CLI thi aavta badha jobs ene share kare che.
 * Darek run mate navu session banaviye to darek run ma 5–10 second bagde.
 */
@Configuration
public class SparkConfig {

    private static final Logger log = LoggerFactory.getLogger(SparkConfig.class);

    /**
     * SparkSession bean. Application bandh thay tyare Spring {@code stop()} call kare che.
     *
     * JVM default timezone pan aahi j set kariye chhiye (IST):
     *   Spark JDBC Postgres na TIMESTAMP (timezone vagar) column ne JVM timezone ma vanche che.
     *   Aapde Postgres ma IST local time lakhiye chhiye, etle vanchti vakhte pan IST hovu joie.
     *   Nahi to SCD2 valid_from / valid_to ma 5:30 kalak no farak aavi jay.
     *
     * Badha spark.* settings yml (etl.spark.config) mathi aave che. Performance mate je settings
     * important che te application.yml ma comment sathe che.
     */
    @Bean(destroyMethod = "stop")
    public SparkSession sparkSession(EtlProperties properties) {
        TimeZone.setDefault(TimeZone.getTimeZone(properties.zoneId()));

        EtlProperties.Spark spark = properties.spark();
        SparkConf conf = new SparkConf()
                .setAppName(spark.appName())
                .setMaster(spark.master())
                // Business timezone: date_format(order_ts,'yyyyMMdd') IST divas aape
                .set("spark.sql.session.timeZone", properties.timezone());

        if (spark.config() != null) {
            spark.config().forEach(conf::set);
        }

        log.info("SparkSession start thay che: master={}, app={}, timezone={}",
                spark.master(), spark.appName(), properties.timezone());
        return SparkSession.builder().config(conf).getOrCreate();
    }
}
