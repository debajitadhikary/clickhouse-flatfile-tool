package com.debajit.clickhouse;

import com.opencsv.CSVReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;
public class ClickHouseClient {
    private final String host;
    private final String port;
    private final String database;
    private final String user;
    private final String jwtToken;

    public ClickHouseClient(String host, String port, String database, String user, String jwtToken) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.user = user;
        this.jwtToken = jwtToken;
    }

    public Connection connect() throws Exception {
        String url = String.format("jdbc:clickhouse://%s:%s/%s", host, port, database);
        return DriverManager.getConnection(url, user, jwtToken);
    }

    public void createTableFromCsv(String csvPath, String tableName) throws Exception {
        CsvSchemaReader reader = new CsvSchemaReader();
        List<CsvSchemaReader.Column> schema = reader.inferSchema(csvPath);
        StringBuilder createQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        createQuery.append(tableName).append(" (");
        for (int i = 0; i < schema.size(); i++) {
            createQuery.append("`").append(schema.get(i).name).append("` ").append(schema.get(i).type);
            if (i < schema.size() - 1) {
                createQuery.append(", ");
            }
        }
        createQuery.append(") ENGINE=MergeTree ORDER BY date");
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + tableName);
            stmt.execute(createQuery.toString());
        }
    }

    public void loadCsvData(String csvPath, String tableName) throws Exception {
        String insertQuery = "INSERT INTO " + tableName + " FORMAT CSVWithNames";
        StringBuilder csvData = new StringBuilder();
        try (CSVReader reader = new CSVReader(new FileReader(csvPath))) {
            String[] headers = reader.readNext(); // Skip header
            for (String[] row; (row = reader.readNext()) != null; ) {
                for (int i = 0; i < row.length; i++) {
                    csvData.append("\"").append(row[i].replace("\"", "\"\"")).append("\"");
                    if (i < row.length - 1) {
                        csvData.append(",");
                    }
                }
                csvData.append("\n");
            }
        }
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute(insertQuery + " SETTINGS input_format_csv_skip_first_line = 1");
            stmt.execute(insertQuery + " " + csvData.toString());
        }
    }

    public List<String> getTables() throws Exception {
        List<String> tables = new ArrayList<>();
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name FROM system.tables WHERE database = '" + database + "'")) {
            while (rs.next()) {
                tables.add(rs.getString("name"));
            }
        }
        return tables;
    }

    public int getRowCount(String tableName) throws Exception {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count() AS cnt FROM " + tableName)) {
            if (rs.next()) {
                return rs.getInt("cnt");
            }
            return 0;
        }
    }

    public static void main(String[] args) {
        try {
            ClickHouseClient client = new ClickHouseClient(
                "localhost", "8123", "default", "default", "debajit-token-123"
            );
            String csvPath = "C:/Users/DEBAJIT/Desktop/clickhouse-flatfile-tool/price_paid.csv";
            String tableName = "uk_price_paid";

            // Create table
            client.createTableFromCsv(csvPath, tableName);
            System.out.println("Table created: " + tableName);

            // Load data
            client.loadCsvData(csvPath, tableName);
            System.out.println("Data loaded into: " + tableName);

            // Verify
            System.out.println("Tables: " + client.getTables());
            System.out.println("Row count: " + client.getRowCount(tableName));
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}