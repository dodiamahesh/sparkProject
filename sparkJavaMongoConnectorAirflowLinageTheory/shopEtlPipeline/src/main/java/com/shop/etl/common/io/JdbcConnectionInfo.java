package com.shop.etl.common.io;

import java.io.Serializable;

/**
 * Spark EXECUTOR par Postgres connection kholva mate jaruri mahiti.
 *
 * Kem alag class? Executor (worker) par Spring beans (DataSource, JdbcTemplate) hota nathi.
 * Spark aa nanu Serializable object darek task sathe worker par mokle che, ane worker
 * pote {@code DriverManager.getConnection(...)} thi connection khole che.
 *
 * @param url       jdbc url. {@code reWriteBatchedInserts=true} hovu joie (yml ma che): pgjdbc
 *                  1000 alag INSERT ne ek multi-row INSERT ma badle che → 5–10 gani fast.
 * @param username  db user
 * @param password  db password
 * @param batchSize ek executeBatch + commit ma ketli rows
 * @param zoneId    business timezone (IST): Instant ne Postgres TIMESTAMP ma lakhti vakhte vapray
 */
public record JdbcConnectionInfo(String url, String username, String password, int batchSize, String zoneId)
        implements Serializable {

    /** Logs ma password na dekhay. */
    @Override
    public String toString() {
        return "JdbcConnectionInfo[url=" + url + ", user=" + username + ", batchSize=" + batchSize + "]";
    }
}
