package com.demo.etl.framework.exception;

/**
 * A source failed a quality gate (too many rejected rows, or schema drift with failOnSchemaDrift()).
 * The job stops BEFORE the collection is loaded and the watermark does not move, so after the source is
 * fixed the next run simply reads the same window again.
 */
public class DataQualityGateException extends EtlException {

    private final String collection;

    public DataQualityGateException(String collection, String message) {
        super(message);
        this.collection = collection;
    }

    public String getCollection() {
        return collection;
    }
}
