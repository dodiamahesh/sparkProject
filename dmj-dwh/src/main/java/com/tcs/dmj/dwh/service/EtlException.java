package com.tcs.dmj.dwh.service;

/**
 * Base class for every ETL error. Unchecked, so pipeline code stays clean.
 */
public class EtlException extends RuntimeException {

    public EtlException(String message) {
        super(message);
    }

    public EtlException(String message, Throwable cause) {
        super(message, cause);
    }
}
