package com.demo.etl.api.dto;

import java.time.LocalDateTime;

/** Same error JSON for every failed request. */
public record ApiError(LocalDateTime timestamp, int status, String error, String message, String path) {
}
