package com.debajit.clickhouse;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CsvSchemaReader {
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

    public List<Column> inferSchema(String filePath) throws IOException, CsvValidationException {
        List<Column> columns = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] header = reader.readNext();
            if (header == null) {
                throw new IOException("CSV file is empty or has no header");
            }
            List<String[]> rows = new ArrayList<>();
            for (int i = 0; i < 10 && reader.peek() != null; i++) {
                rows.add(reader.readNext());
            }
            for (int i = 0; i < header.length; i++) {
                String colName = header[i].replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
                String type = inferColumnType(rows, i);
                columns.add(new ColumnImpl(colName, type));
            }
        }
        return columns;
    }

    private String inferColumnType(List<String[]> rows, int colIndex) {
        boolean allEmpty = true;
        boolean isInteger = true;
        boolean isDate = true;
        for (String[] row : rows) {
            if (colIndex >= row.length || row[colIndex].isEmpty()) {
                continue;
            }
            allEmpty = false;
            String value = row[colIndex];
            try {
                Integer.parseInt(value);
            } catch (NumberFormatException e) {
                isInteger = false;
            }
            if (!value.matches("\\d{4}-\\d{2}-\\d{2}( \\d{2}:\\d{2}:\\d{2})?")) {
                isDate = false;
            }
        }
        if (allEmpty) {
            return "String";
        }
        if (isInteger) {
            return "UInt32";
        }
        if (isDate) {
            return "DateTime";
        }
        return "String";
    }
}