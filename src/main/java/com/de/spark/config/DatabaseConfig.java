package com.de.spark.config;

import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;

/**
 * Two MySQL databases: de_silver (curated + control tables) and de_gold (aggregates).
 * Flyway creates/updates the tables of each database at startup from
 * src/main/resources/db/silver and db/gold.
 */
@Configuration
public class DatabaseConfig {

    @Bean
    @Primary
    public DataSource silverDataSource(PipelineProperties p) {
        DataSource ds = dataSource(p.getSilverJdbcUrl(), p);
        migrate(ds, "classpath:db/silver");
        return ds;
    }

    @Bean
    public DataSource goldDataSource(PipelineProperties p) {
        DataSource ds = dataSource(p.getGoldJdbcUrl(), p);
        migrate(ds, "classpath:db/gold");
        return ds;
    }

    @Bean
    @Primary
    public JdbcTemplate silverJdbc(@Qualifier("silverDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    @Bean
    public JdbcTemplate goldJdbc(@Qualifier("goldDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    /** Runs a block of SQL statements on de_silver as ONE transaction (all or nothing). */
    @Bean
    @Primary
    public TransactionTemplate silverTx(@Qualifier("silverDataSource") DataSource ds) {
        return new TransactionTemplate(new DataSourceTransactionManager(ds));
    }

    @Bean
    public TransactionTemplate goldTx(@Qualifier("goldDataSource") DataSource ds) {
        return new TransactionTemplate(new DataSourceTransactionManager(ds));
    }

    private static DataSource dataSource(String url, PipelineProperties p) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(url);
        ds.setUsername(p.getDbUsername());
        ds.setPassword(p.getDbPassword());
        ds.setMaximumPoolSize(3);
        return ds;
    }

    private static void migrate(DataSource ds, String location) {
        Flyway.configure().dataSource(ds).locations(location).load().migrate();
    }
}
