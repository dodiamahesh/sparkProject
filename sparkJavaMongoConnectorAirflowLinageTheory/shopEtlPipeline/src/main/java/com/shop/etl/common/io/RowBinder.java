package com.shop.etl.common.io;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Ek Spark Row na values ne SQL statement na {@code ?} ma set kare che.
 *
 * Aa interface Serializable che kem ke binder Spark executor par mokalay che.
 *
 * ⚠️ IMPORTANT: binder lambda Spring bean ni andar lakho tyare e {@code this} (bean) ne
 * capture na kare evu rakho. Fakt method parameters / local variables (String, int) vapro.
 * Bean capture thay to Spark "Task not serializable" error aape. Etle binders static
 * methods ma banaviye chhiye (Loader classes jovo).
 */
@FunctionalInterface
public interface RowBinder extends Serializable {

    /**
     * @param ps  prepared statement (je step nu SQL che)
     * @param row Spark row
     * @param v   helper: null safe ane type safe value set karva mate
     */
    void bind(PreparedStatement ps, Row row, RowValues v) throws SQLException;
}
