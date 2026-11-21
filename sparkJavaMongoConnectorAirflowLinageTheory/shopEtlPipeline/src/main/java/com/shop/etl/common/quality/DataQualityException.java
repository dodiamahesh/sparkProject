package com.shop.etl.common.quality;

/**
 * Quality gate fail thay tyare. Job FAILED thay ane watermark aagal nathi vadhatu.
 * Message audit table na error_message ma lakhay che.
 */
public class DataQualityException extends RuntimeException {

    public DataQualityException(String message) {
        super(message);
    }
}
