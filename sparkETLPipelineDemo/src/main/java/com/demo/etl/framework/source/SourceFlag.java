package com.demo.etl.framework.source;

/**
 * Values of the processedFlag field in every MongoDB document.
 *
 * The source application inserts documents with 'N' (or without the field).
 * When it changes a document it sets the flag back to 'N', so the next INCREMENTAL run picks it up.
 */
public enum SourceFlag {

    NEW("N"),
    LOADED("Y"),
    REJECTED("E"),
    DUPLICATE("D");

    private final String code;

    SourceFlag(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }
}
