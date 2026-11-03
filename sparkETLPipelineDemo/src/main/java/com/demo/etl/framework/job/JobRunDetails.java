package com.demo.etl.framework.job;

import com.demo.etl.framework.audit.JobRun;
import com.demo.etl.framework.audit.JobStep;

import java.util.List;

/** A job run together with all its steps (REST response). */
public record JobRunDetails(JobRun job, List<JobStep> steps) {
}
