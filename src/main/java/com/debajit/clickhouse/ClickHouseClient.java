package com.debajit.clickhouse;

import com.opencsv.CSVReader;
import org.apache.commons.cli.*;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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
        createQuery.append(") ENGINE=MergeTree ORDER BY ");
        // Use date if available, else first column
        String orderBy = schema.stream()
                .filter(col -> col.type.equals("DateTime") || col.type.equals("Date"))
                .findFirst()
                .map(col -> col.name)
                .orElse(schema.get(0).name);
        createQuery.append(orderBy);
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + tableName);
            stmt.execute(createQuery.toString());
        }
    }

    public void loadCsvData(String csvPath, String tableName) throws Exception {
        String insertQuery = "INSERT INTO " + tableName + " FORMAT CSVWithNames";
        try (Connection conn = connect(); Statement stmt = conn.createStatement();
             CSVReader reader = new CSVReader(new FileReader(csvPath))) {
            stmt.execute(insertQuery + " SETTINGS input_format_csv_skip_first_line = 1");
            StringBuilder csvData = new StringBuilder();
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
        Options options = new Options();
        options.addOption("c", "csv", true, "Path to CSV file");
        options.addOption("t", "table", true, "Table name");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            if (!cmd.hasOption("csv") || !cmd.hasOption("table")) {
                throw new ParseException("Missing required arguments: --csv and --table");
            }

            String csvPath = cmd.getOptionValue("csv");
            String tableName = cmd.getOptionValue("table");

            ClickHouseClient client = new ClickHouseClient(
                "localhost", "8123", "default", "default", "debajit-token-123"
            );

            // Create table
            System.out.println("Creating table: " + tableName);
            client.createTableFromCsv(csvPath, tableName);

            // Load data
            System.out.println("Loading data into: " + tableName);
            client.loadCsvData(csvPath, tableName);

            // Verify
            System.out.println("Tables: " + client.getTables());
            System.out.println("Row count for " + tableName + ": " + client.getRowCount(tableName));

        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -jar clickhouse-flatfile-tool.jar", options);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}