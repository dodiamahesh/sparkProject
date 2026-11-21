package com.shop.etl.common.io;

import com.shop.etl.common.config.EtlProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Spark Dataset ne Postgres ma DIRECT (staging vagar) lakhva no common component.
 *
 * Design doc Section 12 mujab aapde staging table nathi vaparta. Darek target table par unique
 * business key che, etle {@code INSERT ... ON CONFLICT DO UPDATE} (upsert) thi sidhu lakhiye.
 *
 * Kem Spark nu {@code df.write().jdbc()} nahi?
 *   - Ema fakt APPEND ke OVERWRITE che. Upsert nathi.
 *   - OVERWRITE table ne drop/truncate kare che. Incremental load ma juno data jato rahe.
 *   Etle aapde potano foreachPartition writer vapariye chhiye (JdbcPartitionWriter).
 *
 * Parallelism: {@code coalesce(writeParallelism)} thi ek sathe ketla DB connections khulshe e
 * control thay che. Vadhu connections = DB par lock/CPU nu dabaan; ochha = dheemu.
 * coalesce shuffle nathi karto (repartition kare), etle sasto che.
 */
@Component
public class JdbcBatchWriter {

    private static final Logger log = LoggerFactory.getLogger(JdbcBatchWriter.class);

    private final JdbcConnectionInfo connection;
    private final int writeParallelism;

    public JdbcBatchWriter(EtlProperties properties) {
        EtlProperties.Postgres pg = properties.postgres();
        this.connection = new JdbcConnectionInfo(pg.url(), pg.username(), pg.password(),
                pg.writeBatchSize(), properties.timezone());
        this.writeParallelism = pg.writeParallelism();
    }

    /**
     * Dataset ni darek row mate aapela steps (SQL) chalave che.
     *
     * Aa ek Spark ACTION che: aahi j kharekhar Mongo read, transform ane write chale che.
     *
     * @param target logs mate table naam
     * @param data   lakhvano data (columns binder ma vaparata naam sathe)
     * @param steps  ek ke vadhare SQL steps (order mujab chale)
     */
    public void write(String target, Dataset<Row> data, List<JdbcStep> steps) {
        log.info("JDBC write start: target={}, steps={}, parallelism={}, batchSize={}",
                target, steps.stream().map(JdbcStep::name).toList(), writeParallelism, connection.batchSize());
        data.coalesce(writeParallelism)
                .foreachPartition(new JdbcPartitionWriter(connection, steps));
        log.info("JDBC write puru: target={}", target);
    }
}
