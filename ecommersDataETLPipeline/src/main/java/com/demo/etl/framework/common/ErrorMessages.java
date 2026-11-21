package com.demo.etl.framework.common;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Turns exceptions into short, readable text for the job audit table.
 */
public final class ErrorMessages {

    private static final int MAX_MESSAGE_LENGTH = 1000;
    private static final int MAX_STACK_TRACE_LENGTH = 4000;

    private ErrorMessages() {
    }

    /** The message of the deepest cause, e.g. "Connection refused". */
    public static String rootCauseMessage(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getClass().getSimpleName() + ": " + root.getMessage();
        return truncate(message, MAX_MESSAGE_LENGTH);
    }

    /** The top-level message (our own exceptions already contain the step name + root cause). */
    public static String shortMessage(Throwable error) {
        String message = error.getMessage() != null ? error.getMessage() : error.getClass().getSimpleName();
        return truncate(message, MAX_MESSAGE_LENGTH);
    }

    public static String stackTrace(Throwable error) {
        StringWriter writer = new StringWriter();
        error.printStackTrace(new PrintWriter(writer));
        return truncate(writer.toString(), MAX_STACK_TRACE_LENGTH);
    }

    private static String truncate(String text, int maxLength) {
        return text.length() <= maxLength ? text : text.substring(0, maxLength) + "...";
    }
}
