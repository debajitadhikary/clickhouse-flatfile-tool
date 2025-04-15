package com.debajit;

import com.debajit.clickhouse.ClickHouseClient;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class IngestionController {
    @PostMapping("/connect")
    public Map<String, Object> connect(@RequestBody Map<String, String> config) throws Exception {
        ClickHouseClient client = new ClickHouseClient(
            config.get("host"), config.get("port"), config.get("database"),
            config.get("user"), config.get("jwtToken")
        );
        return Map.of("status", "Connected", "tables", client.getTables());
    }

    @PostMapping("/schema")
    public List<Column> getSchema(@RequestParam("source") String source,
                                  @RequestParam(value = "file", required = false) MultipartFile file,
                                  @RequestParam(value = "table", required = false) String table) throws Exception {
        ClickHouseClient client = new ClickHouseClient(/* from config */);
        if (source.equals("flatfile") && file != null) {
            String filePath = saveFile(file);
            return client.inferSchema(filePath);
        } else {
            // Fetch ClickHouse table schema
            return List.of();
        }
    }

    @PostMapping("/ingest")
    public Map<String, Object> ingest(@RequestParam("source") String source,
                                     @RequestParam("table") String table,
                                     @RequestParam(value = "file", required = false) MultipartFile file,
                                     @RequestParam("columns") List<String> columns) throws Exception {
        ClickHouseClient client = new ClickHouseClient(/* from config */);
        int count;
        if (source.equals("flatfile")) {
            String filePath = saveFile(file);
            client.createTableFromFile(filePath, table, columns);
            client.loadFileData(filePath, table);
            count = client.getRowCount(table);
        } else {
            String filePath = "output.csv";
            count = client.exportToFile(table, filePath, columns);
        }
        return Map.of("status", "Completed", "count", count);
    }

    private String saveFile(MultipartFile file) throws Exception {
        // Save to temp file
        return "/tmp/" + file.getOriginalFilename();
    }
}