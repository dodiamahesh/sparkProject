package com.tcs.dmj.dwh.utils;

/** Exception -> short text for meta.meta_pipeline_run (error_code, error_message). */
public final class ErrorMessages {

    private static final int MAX_MESSAGE_LENGTH = 1000;

    private ErrorMessages() {
    }

    public static String shortMessage(Throwable error) {
        String message = error.getMessage() != null ? error.getMessage() : error.getClass().getSimpleName();
        return truncate(message);
    }

    /** "DataQualityGateException", or "TopException/RootCauseException" when they differ. */
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

    private static String truncate(String text) {
        return text.length() <= MAX_MESSAGE_LENGTH ? text : text.substring(0, MAX_MESSAGE_LENGTH - 3) + "...";
    }
}
