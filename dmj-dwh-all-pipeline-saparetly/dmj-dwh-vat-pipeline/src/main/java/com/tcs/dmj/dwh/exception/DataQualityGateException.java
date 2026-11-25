package com.tcs.dmj.dwh.exception;

/** Too many rejected rows in one collection: the run stops before that collection is loaded. */
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
