package com.demo.etl.framework.job;

import com.demo.etl.framework.exception.PipelineAlreadyRunningException;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Makes sure the SAME pipeline never runs twice at the same time (different pipelines can run in parallel).
 *
 * Note: this lock lives in memory, which is fine for one application instance.
 * If you run several instances, replace it with a database lock (e.g. ShedLock).
 */
@Component
public class JobLockManager {

    private final Set<String> runningPipelines = ConcurrentHashMap.newKeySet();

    public void acquire(String pipelineName) {
        if (!runningPipelines.add(pipelineName)) {
            throw new PipelineAlreadyRunningException(pipelineName);
        }
    }

    public void release(String pipelineName) {
        runningPipelines.remove(pipelineName);
    }

    public boolean isRunning(String pipelineName) {
        return runningPipelines.contains(pipelineName);
    }
}
