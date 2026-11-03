package com.demo.etl.framework.exception;

import java.util.Collection;

public class PipelineNotFoundException extends EtlException {

    public PipelineNotFoundException(String pipelineName, Collection<String> availablePipelines) {
        super("Pipeline '" + pipelineName + "' not found. Available pipelines: " + availablePipelines);
    }
}
