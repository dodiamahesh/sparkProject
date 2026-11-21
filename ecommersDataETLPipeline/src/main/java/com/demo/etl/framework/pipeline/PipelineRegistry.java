package com.demo.etl.framework.pipeline;

import com.demo.etl.framework.exception.PipelineNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Finds every {@link EtlPipeline} bean automatically (Spring injects the list).
 * You never register a pipeline by hand: add @Component to the class and it is here.
 */
@Component
public class PipelineRegistry {

    private static final Logger log = LoggerFactory.getLogger(PipelineRegistry.class);

    private final Map<String, EtlPipeline> pipelinesByName = new TreeMap<>();

    public PipelineRegistry(List<EtlPipeline> pipelines) {
        for (EtlPipeline pipeline : pipelines) {
            EtlPipeline existing = pipelinesByName.put(pipeline.name(), pipeline);
            if (existing != null) {
                throw new IllegalStateException("Two pipelines use the same name: " + pipeline.name());
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
