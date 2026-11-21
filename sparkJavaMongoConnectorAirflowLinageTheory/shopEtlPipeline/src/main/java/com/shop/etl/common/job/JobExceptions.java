package com.shop.etl.common.job;

/**
 * Job launch vakhte aavti exceptions. ApiExceptionHandler emne HTTP status ma badle che.
 * Ek file ma rakhi che kem ke nani che ane sathe samjay.
 */
public final class JobExceptions {

    private JobExceptions() {
    }

    /** Aavo job registered nathi → HTTP 404. */
    public static class JobNotFound extends RuntimeException {
        public JobNotFound(String jobName) {
            super("Job '" + jobName + "' nathi. Available jobs: GET /api/jobs");
        }
    }

    /** yml ma enabled=false → HTTP 409. */
    public static class JobDisabled extends RuntimeException {
        public JobDisabled(String jobName) {
            super("Job '" + jobName + "' yml ma bandh (enabled: false) che");
        }
    }

    /** Same job pehla thi chale che (lock nathi malyo) → HTTP 409. */
    public static class JobAlreadyRunning extends RuntimeException {
        public JobAlreadyRunning(String jobName) {
            super("Job '" + jobName + "' haju chale che. Puro thay pachhi farithi try karo.");
        }
    }

    /** Async queue bharai gai → HTTP 503. */
    public static class JobQueueFull extends RuntimeException {
        public JobQueueFull(String jobName) {
            super("Job '" + jobName + "' mate queue bharai gai che. Thodi vaar pachhi try karo.");
        }
    }
}
