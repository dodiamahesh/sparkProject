package com.tcs.dmj.dwh.core;

import com.tcs.dmj.dwh.exception.PipelineConfigurationException;
import com.tcs.dmj.dwh.exception.PipelineNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** All EtlPipeline beans by name (Spring injects them: a @Component pipeline is registered automatically). */
@Component
public class PipelineRegistry {

    private static final Logger log = LoggerFactory.getLogger(PipelineRegistry.class);

    private final Map<String, EtlPipeline> pipelinesByName = new TreeMap<>();

    public PipelineRegistry(List<EtlPipeline> pipelines) {
        for (EtlPipeline pipeline : pipelines) {
            if (pipelinesByName.putIfAbsent(pipeline.name(), pipeline) != null) {
                throw new PipelineConfigurationException("Two pipelines use the same name: " + pipeline.name());
            }
            log.info("Registered ETL pipeline '{}' ({})", pipeline.name(), pipeline.getClass().getSimpleName());
        }
    }

    public EtlPipeline getPipeline(String name) {
        EtlPipeline pipeline = pipelinesByName.get(name);
        if (pipeline == null) {
            throw new PipelineNotFoundException(name, pipelinesByName.keySet());
        }
        return pipeline;
    }

    public Collection<EtlPipeline> getAllPipelines() {
        return pipelinesByName.values();
    }
}
