package com.tcs.dmj.dwh.utils;

/**
 * Turns exceptions into short, readable text for meta.meta_pipeline_run (error_code, error_message).
 */
public final class ErrorMessages {

    private static final int MAX_MESSAGE_LENGTH = 1000;

    private ErrorMessages() {
    }

    /** The message of the deepest cause, e.g. "PSQLException: Connection refused". */
    public static String rootCauseMessage(Throwable error) {
        Throwable root = rootCause(error);
        String message = root.getClass().getSimpleName() + ": " + root.getMessage();
        return truncate(message, MAX_MESSAGE_LENGTH);
    }

    /** The top-level message (our own exceptions already contain the root cause). */
    public static String shortMessage(Throwable error) {
        String message = error.getMessage() != null ? error.getMessage() : error.getClass().getSimpleName();
        return truncate(message, MAX_MESSAGE_LENGTH);
    }

    /**
     * error_code of a failed run: the class of the top-level exception and, when different, of the root cause,
     * e.g. "DataQualityGateException" or "UncategorizedSQLException/PSQLException".
     */
    public static String errorCode(Throwable error) {
        String top = error.getClass().getSimpleName();
        String root = rootCause(error).getClass().getSimpleName();
        return top.equals(root) ? top : top + "/" + root;
    }

    private static Throwable rootCause(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root;
    }

    private static String truncate(String text, int maxLength) {
        return text.length() <= maxLength ? text : text.substring(0, maxLength - 3) + "...";
    }
}
