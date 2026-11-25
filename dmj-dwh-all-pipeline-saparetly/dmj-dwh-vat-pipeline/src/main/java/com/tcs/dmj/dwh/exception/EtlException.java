package com.tcs.dmj.dwh.exception;

/** Base class of every ETL error (unchecked). */
public class EtlException extends RuntimeException {

    public EtlException(String message) {
        super(message);
    }

    public EtlException(String message, Throwable cause) {
        super(message, cause);
    }
}
