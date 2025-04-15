package com.debajit.clickhouse;

import com.opencsv.CSVReader;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseClient {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseClient.class);
    private static final int BATCH_SIZE = 1000;
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 1000;

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

    public Connection connect() throws SQLException {
        String url = String.format("jdbc:clickhouse://%s:%s/%s", host, port, database);
        return DriverManager.getConnection(url, user, jwtToken);
    }

    public void createTableFromCsv(String csvPath, String tableName) throws Exception {
        logger.info("Inferring schema for CSV: {}", csvPath);
        CsvSchemaReader reader = new CsvSchemaReader();
        List<CsvSchemaReader.Column> schema;
        try {
            schema = reader.inferSchema(csvPath);
        } catch (Exception e) {
            logger.error("Failed to infer schema: {}", e.getMessage());
            throw new Exception("Schema inference failed", e);
        }

        StringBuilder createQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        createQuery.append(tableName).append(" (");
        for (int i = 0; i < schema.size(); i++) {
            createQuery.append("`").append(schema.get(i).name).append("` ").append(schema.get(i).type);
            if (i < schema.size() - 1) {
                createQuery.append(", ");
            }
        }
        createQuery.append(") ENGINE=MergeTree ORDER BY ");
        String orderBy = schema.stream()
                .filter(col -> col.type.equals("DateTime") || col.type.equals("Date"))
                .findFirst()
                .map(col -> col.name)
                .orElse(schema.get(0).name);
        createQuery.append(orderBy);

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
                logger.info("Attempt {}: Creating table {}", attempt, tableName);
                stmt.execute("DROP TABLE IF EXISTS " + tableName);
                stmt.execute(createQuery.toString());
                logger.info("Table {} created successfully", tableName);
                return;
            } catch (SQLException e) {
                logger.warn("Attempt {} failed: {}", attempt, e.getMessage());
                if (attempt == MAX_RETRIES) {
                    logger.error("Failed to create table after {} attempts", MAX_RETRIES);
                    throw new Exception("Table creation failed", e);
                }
                Thread.sleep(RETRY_DELAY_MS);
            }
        }
    }

    public void loadCsvData(String csvPath, String tableName) throws Exception {
        logger.info("Loading data from {} into {}", csvPath, tableName);
        String insertQuery = "INSERT INTO " + tableName + " FORMAT CSV";
        List<String> batch = new ArrayList<>();
        int rowNum = 0;
        int errors = 0;

        try (CSVReader reader = new CSVReader(new FileReader(csvPath))) {
            String[] headers = reader.readNext(); // Skip header
            rowNum++;

            for (String[] row; (row = reader.readNext()) != null; rowNum++) {
                try {
                    StringBuilder csvRow = new StringBuilder();
                    for (int i = 0; i < row.length; i++) {
                        csvRow.append("\"").append(row[i].replace("\"", "\"\"")).append("\"");
                        if (i < row.length - 1) {
                            csvRow.append(",");
                        }
                    }
                    batch.add(csvRow.toString());

                    if (batch.size() >= BATCH_SIZE) {
                        executeBatch(insertQuery, batch, tableName);
                        batch.clear();
                    }
                } catch (Exception e) {
                    logger.error("Skipping malformed row {}: {}", rowNum, e.getMessage());
                    errors++;
                }
            }

            // Load remaining rows
            if (!batch.isEmpty()) {
                executeBatch(insertQuery, batch, tableName);
            }

            logger.info("Loaded {} rows with {} errors", rowNum - 1 - errors, errors);
            if (errors > 0) {
                logger.warn("Encountered {} row errors during loading", errors);
            }
        }
    }

    private void executeBatch(String insertQuery, List<String> batch, String tableName) throws Exception {
        StringBuilder csvData = new StringBuilder();
        for (String row : batch) {
            csvData.append(row).append("\n");
        }

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
                logger.debug("Attempt {}: Inserting batch of {} rows", attempt, batch.size());
                stmt.execute(insertQuery + " " + csvData.toString());
                logger.debug("Batch inserted successfully");
                return;
            } catch (SQLException e) {
                logger.warn("Batch insert attempt {} failed: {}", attempt, e.getMessage());
                if (attempt == MAX_RETRIES) {
                    logger.error("Failed to insert batch after {} attempts", MAX_RETRIES);
                    throw new Exception("Batch insert failed", e);
                }
                Thread.sleep(RETRY_DELAY_MS);
            }
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
            logger.info("Starting table creation for {}", tableName);
            client.createTableFromCsv(csvPath, tableName);

            // Load data
            logger.info("Starting data load for {}", tableName);
            client.loadCsvData(csvPath, tableName);

            // Verify
            List<String> tables = client.getTables();
            int rowCount = client.getRowCount(tableName);
            logger.info("Tables: {}", tables);
            logger.info("Row count for {}: {}", tableName, rowCount);

        } catch (ParseException e) {
            logger.error("CLI error: {}", e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -jar clickhouse-flatfile-tool.jar", options);
        } catch (Exception e) {
            logger.error("Processing error: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}