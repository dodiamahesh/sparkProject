package com.shop.etl.common.trigger;

import com.shop.etl.common.job.JobExceptions;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.Map;

/**
 * API errors ne saaf HTTP status + JSON message ma badle che.
 * Caller (Airflow) status code thi samji shake: 409 = chalu che (pachhi try), 404 = khoto naam.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    /** Job naam khotu → 404. */
    @ExceptionHandler(JobExceptions.JobNotFound.class)
    public ResponseEntity<Map<String, Object>> notFound(RuntimeException e) {
        return error(HttpStatus.NOT_FOUND, e);
    }

    /** Job chalu che ke bandh che → 409 Conflict. */
    @ExceptionHandler({JobExceptions.JobAlreadyRunning.class, JobExceptions.JobDisabled.class})
    public ResponseEntity<Map<String, Object>> conflict(RuntimeException e) {
        return error(HttpStatus.CONFLICT, e);
    }

    /** Queue bharai gai → 503 (thodi vaar pachhi try). */
    @ExceptionHandler(JobExceptions.JobQueueFull.class)
    public ResponseEntity<Map<String, Object>> busy(RuntimeException e) {
        return error(HttpStatus.SERVICE_UNAVAILABLE, e);
    }

    /** Khoto input (windowEnd format) → 400. */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> badRequest(RuntimeException e) {
        return error(HttpStatus.BAD_REQUEST, e);
    }

    private static ResponseEntity<Map<String, Object>> error(HttpStatus status, RuntimeException e) {
        return ResponseEntity.status(status).body(Map.of(
                "status", status.value(),
                "error", status.getReasonPhrase(),
                "message", String.valueOf(e.getMessage()),
                "time", Instant.now().toString()));
    }
}
