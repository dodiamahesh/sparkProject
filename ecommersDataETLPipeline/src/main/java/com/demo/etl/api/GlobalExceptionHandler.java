package com.demo.etl.api;

import com.demo.etl.api.dto.ApiError;
import com.demo.etl.framework.exception.EtlException;
import com.demo.etl.framework.exception.JobRunNotFoundException;
import com.demo.etl.framework.exception.PipelineAlreadyRunningException;
import com.demo.etl.framework.exception.PipelineNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.time.LocalDateTime;

/**
 * Turns exceptions into clean JSON errors with the right HTTP status.
 * Note: a job that FAILS is not an HTTP error - the job row simply gets status FAILED.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler({PipelineNotFoundException.class, JobRunNotFoundException.class})
    public ResponseEntity<ApiError> handleNotFound(EtlException e, HttpServletRequest request) {
        return build(HttpStatus.NOT_FOUND, e.getMessage(), request);
    }

    @ExceptionHandler(PipelineAlreadyRunningException.class)
    public ResponseEntity<ApiError> handleAlreadyRunning(PipelineAlreadyRunningException e, HttpServletRequest request) {
        return build(HttpStatus.CONFLICT, e.getMessage(), request);
    }

    @ExceptionHandler({IllegalArgumentException.class, MethodArgumentTypeMismatchException.class})
    public ResponseEntity<ApiError> handleBadRequest(Exception e, HttpServletRequest request) {
        return build(HttpStatus.BAD_REQUEST, e.getMessage(), request);
    }

    @ExceptionHandler(EtlException.class)
    public ResponseEntity<ApiError> handleEtl(EtlException e, HttpServletRequest request) {
        log.error("ETL error on {}", request.getRequestURI(), e);
        return build(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage(), request);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpected(Exception e, HttpServletRequest request) {
        log.error("Unexpected error on {}", request.getRequestURI(), e);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "Unexpected error: " + e.getMessage(), request);
    }

    private ResponseEntity<ApiError> build(HttpStatus status, String message, HttpServletRequest request) {
        ApiError body = new ApiError(LocalDateTime.now(), status.value(), status.getReasonPhrase(),
                message, request.getRequestURI());
        return ResponseEntity.status(status).body(body);
    }
}
