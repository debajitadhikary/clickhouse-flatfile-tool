package com.debajit.clickhouse;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CsvSchemaReader {
    public static class Column {
        String name;
        String type;

        public Column(String name, String type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public String toString() {
            return name + " " + type;
        }
    }

    public List<Column> inferSchema(String filePath) throws IOException, CsvValidationException {
        List<Column> columns = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            // Read header
            String[] header = reader.readNext();
            if (header == null) {
                throw new IOException("CSV file is empty or has no header");
            }

            // Read a few rows to infer types
            List<String[]> rows = new ArrayList<>();
            for (int i = 0; i < 10 && reader.peek() != null; i++) {
                rows.add(reader.readNext());
            }

            // Infer types for each column
            for (int i = 0; i < header.length; i++) {
                String colName = header[i].replaceAll("[^a-zA-Z0-9_]", "_");
                String type = inferColumnType(rows, i);
                columns.add(new Column(colName, type));
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

            // Check for integer
            try {
                Integer.parseInt(value);
            } catch (NumberFormatException e) {
                isInteger = false;
            }

            // Check for date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
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

    public static void main(String[] args) {
        try {
            CsvSchemaReader reader = new CsvSchemaReader();
            List<Column> schema = reader.inferSchema("C:/Users/DEBAJIT/Desktop/clickhouse-flatfile-tool/price_paid.csv");
            System.out.println("Inferred Schema:");
            for (Column col : schema) {
                System.out.println(col);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}