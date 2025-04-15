package com.debajit.clickhouse;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JsonSchemaReader {
    public static class ColumnImpl implements Column {
        private final String name;
        private final String type;

        public ColumnImpl(String name, String type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public String toString() {
            return name + " " + type;
        }
    }

    public List<Column> inferSchema(String filePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<Column> columns = new ArrayList<>();
        List<Map<String, Object>> records = mapper.readValue(new File(filePath), List.class);

        if (records.isEmpty()) {
            throw new Exception("JSON file is empty");
        }

        Map<String, Object> firstRecord = records.get(0);
        for (Map.Entry<String, Object> entry : firstRecord.entrySet()) {
            String colName = entry.getKey().replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
            String type = inferType(entry.getValue(), records);
            columns.add(new ColumnImpl(colName, type));
        }

        return columns;
    }

    private String inferType(Object value, List<Map<String, Object>> records) {
        if (value == null) {
            return "String";
        }
        if (value instanceof Integer || value instanceof Long) {
            return "UInt32";
        }
        if (value instanceof String str) {
            if (str.matches("\\d{4}-\\d{2}-\\d{2}( \\d{2}:\\d{2}:\\d{2})?")) {
                return "DateTime";
            }
        }
        return "String";
    }
}