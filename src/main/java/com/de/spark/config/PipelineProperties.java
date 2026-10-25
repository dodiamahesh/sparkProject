package com.de.spark.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/** All "pipeline.*" settings from application.properties in one place. */
@Configuration
@ConfigurationProperties(prefix = "pipeline")
public class PipelineProperties {

    private String mongoUri;
    private String bronzeDatabase;
    private String silverJdbcUrl;
    private String goldJdbcUrl;
    private String dbUsername;
    private String dbPassword;
    private int safetyLagMinutes;
    private int partitions;
    private int jdbcBatchSize;

    /** JDBC URL for Spark writes: MySQL sends a batch as ONE multi-row INSERT only with this flag. */
    public String sparkUrl(String jdbcUrl) {
        return jdbcUrl + (jdbcUrl.contains("?") ? "&" : "?") + "rewriteBatchedStatements=true";
    }

    /** Connection properties for Spark JDBC reads/writes. */
    public Properties sparkJdbcProps() {
        Properties p = new Properties();
        p.put("user", dbUsername);
        p.put("password", dbPassword);
        p.put("driver", "com.mysql.cj.jdbc.Driver");
        p.put("batchsize", String.valueOf(jdbcBatchSize));
        return p;
    }

    public String getMongoUri() { return mongoUri; }
    public void setMongoUri(String mongoUri) { this.mongoUri = mongoUri; }
    public String getBronzeDatabase() { return bronzeDatabase; }
    public void setBronzeDatabase(String bronzeDatabase) { this.bronzeDatabase = bronzeDatabase; }
    public String getSilverJdbcUrl() { return silverJdbcUrl; }
    public void setSilverJdbcUrl(String silverJdbcUrl) { this.silverJdbcUrl = silverJdbcUrl; }
    public String getGoldJdbcUrl() { return goldJdbcUrl; }
    public void setGoldJdbcUrl(String goldJdbcUrl) { this.goldJdbcUrl = goldJdbcUrl; }
    public String getDbUsername() { return dbUsername; }
    public void setDbUsername(String dbUsername) { this.dbUsername = dbUsername; }
    public String getDbPassword() { return dbPassword; }
    public void setDbPassword(String dbPassword) { this.dbPassword = dbPassword; }
    public int getSafetyLagMinutes() { return safetyLagMinutes; }
    public void setSafetyLagMinutes(int safetyLagMinutes) { this.safetyLagMinutes = safetyLagMinutes; }
    public int getPartitions() { return partitions; }
    public void setPartitions(int partitions) { this.partitions = partitions; }
    public int getJdbcBatchSize() { return jdbcBatchSize; }
    public void setJdbcBatchSize(int jdbcBatchSize) { this.jdbcBatchSize = jdbcBatchSize; }
}
