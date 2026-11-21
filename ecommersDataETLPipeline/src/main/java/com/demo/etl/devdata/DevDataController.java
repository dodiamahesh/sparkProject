package com.demo.etl.devdata;

import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DEV ONLY (this bean does not exist in prod): start your test again from zero.
 *
 *   POST /api/dev/reset?cleanPostgres=true
 *     - drops + reloads every MongoDB sample collection (all flags back to 'N')
 *     - cleanPostgres=true also empties every silver and etl_audit table
 */
@Profile("dev")
@RestController
@RequestMapping("/api/dev")
public class DevDataController {

    private final SampleDataLoader sampleDataLoader;
    private final JdbcTemplate jdbc;

    public DevDataController(SampleDataLoader sampleDataLoader, JdbcTemplate jdbc) {
        this.sampleDataLoader = sampleDataLoader;
        this.jdbc = jdbc;
    }

    @PostMapping("/reset")
    public Map<String, Object> reset(@RequestParam(defaultValue = "true") boolean cleanPostgres) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mongoDocumentsLoaded", sampleDataLoader.load(true));

        if (cleanPostgres) {
            List<String> tables = jdbc.queryForList("""
                    SELECT table_schema || '.' || table_name
                      FROM information_schema.tables
                     WHERE table_schema IN ('silver', 'etl_audit', 'etl_staging')
                       AND table_type = 'BASE TABLE'
                    """, String.class);
            if (!tables.isEmpty()) {
                jdbc.execute("TRUNCATE TABLE " + String.join(", ", tables) + " RESTART IDENTITY CASCADE");
            }
            result.put("postgresTablesTruncated", tables);
        }
        return result;
    }
}
