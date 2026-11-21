package com.demo.etl.framework.audit;

import com.demo.etl.framework.common.ErrorMessages;
import com.demo.etl.framework.exception.EtlStepException;
import com.demo.etl.framework.pipeline.PipelineContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * Wraps a piece of work as a "step": saves start time, end time, record count and status in
 * etl_audit.etl_job_step. When the work throws, the step is marked FAILED and the error is
 * re-thrown as {@link EtlStepException} (which names the step).
 */
@Component
public class StepTracker {

    private static final Logger log = LoggerFactory.getLogger(StepTracker.class);

    private final JobStepRepository stepRepository;

    public StepTracker(JobStepRepository stepRepository) {
        this.stepRepository = stepRepository;
    }

    /** For work that returns a record count, e.g. "load table" -> rows written. */
    public long track(PipelineContext context, String stepName, LongSupplier work) {
        return track(context, stepName, work::getAsLong, Long::longValue);
    }

    /** For work that returns an object; {@code recordCounter} says which number to store. */
    public <T> T track(PipelineContext context, String stepName, Supplier<T> work, ToLongFunction<T> recordCounter) {
        long stepId = stepRepository.startStep(context.jobRunId(), context.nextStepOrder(), stepName);
        log.info("[job {}] >> {}", context.jobRunId(), stepName);
        try {
            T result = work.get();
            long recordCount = recordCounter.applyAsLong(result);
            stepRepository.finishStep(stepId, StepStatus.SUCCESS, recordCount, null);
            log.info("[job {}] << {} : {} records", context.jobRunId(), stepName, recordCount);
            return result;
        } catch (RuntimeException e) {
            stepRepository.finishStep(stepId, StepStatus.FAILED, null, ErrorMessages.rootCauseMessage(e));
            throw e instanceof EtlStepException ? e : new EtlStepException(stepName, e);
        }
    }
}
