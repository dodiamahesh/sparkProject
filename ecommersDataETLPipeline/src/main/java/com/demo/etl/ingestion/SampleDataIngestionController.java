package com.demo.etl.ingestion;

import com.demo.etl.ingestion.SampleDataIngestionService.IngestionMode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Data ingestion API: loads the sample JSON files into MongoDB.
 *
 *   GET  /api/ingestion/sample-data/files                       -> files that can be ingested
 *   POST /api/ingestion/sample-data?mode=REPLACE                -> ingest ALL files
 *   POST /api/ingestion/sample-data/{fileName}?mode=REPLACE     -> ingest ONE file (e.g. payments.json)
 *
 * mode = REPLACE (default) | APPEND | SKIP_IF_EXISTS
 * HTTP status: 200 all files OK, 207 some files failed, 500 every file failed.
 */
@RestController
@RequestMapping("/api/ingestion/sample-data")
public class SampleDataIngestionController {

    private final SampleDataIngestionService ingestionService;

    public SampleDataIngestionController(SampleDataIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @GetMapping("/files")
    public List<String> listFiles() {
        return ingestionService.listFiles();
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> ingestAll(@RequestParam(defaultValue = "REPLACE") String mode) {
        Map<String, Object> result = ingestionService.ingestAll(IngestionMode.from(mode));
        return ResponseEntity.status(httpStatus(result)).body(result);
    }

    @PostMapping("/{fileName}")
    public ResponseEntity<Map<String, Object>> ingestOne(@PathVariable String fileName,
                                                         @RequestParam(defaultValue = "REPLACE") String mode) {
        Map<String, Object> result = ingestionService.ingestOne(fileName, IngestionMode.from(mode));
        return ResponseEntity.status(httpStatus(result)).body(result);
    }

    private HttpStatus httpStatus(Map<String, Object> result) {
        return switch ((String) result.get("status")) {
            case "SUCCESS" -> HttpStatus.OK;
            case "PARTIAL_SUCCESS" -> HttpStatus.MULTI_STATUS;
            default -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
    }
}
