package com.debajit.clickhouse;

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
        // Register the driver class explicitly
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver"); // Load the correct driver class

        String url = "jdbc:clickhouse://localhost:8123/default"; // Corrected JDBC URL
        return DriverManager.getConnection(url, user, jwtToken);
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

    public static void main(String[] args) {
        try {
            ClickHouseClient client = new ClickHouseClient(
                "localhost", "8123", "default", "default", "your-secret-jwt-token"
            );
            List<String> tables = client.getTables();
            System.out.println("Tables: " + tables);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
