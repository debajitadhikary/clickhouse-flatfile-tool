# ClickHouse-FlatFile Ingestion Tool

![Build Status](https://img.shields.io/badge/build-passing-brightgreen)
![Java](https://img.shields.io/badge/Java-17-blue)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.5-green)
![ClickHouse](https://img.shields.io/badge/ClickHouse-24.8-orange)

A **bidirectional data ingestion web application** for seamless data transfer between **ClickHouse** and **flat files** (CSV/JSON). Built with **Java**, **Spring Boot**, and a modern, responsive web UI, it offers dynamic schema inference, column selection, JWT authentication, and efficient batch processing. Designed for data engineers, it’s scalable, user-friendly, and production-ready. 🚀

---

## 🌟 Features

- **Bidirectional Data Flow**:
  - **Flat File → ClickHouse**: Ingests CSV/JSON with automatic schema detection.
  - **ClickHouse → Flat File**: Exports tables to CSV with customizable columns.
- **Web Interface**:
  - Intuitive UI with source selection (ClickHouse or Flat File).
  - Config inputs for ClickHouse (Host, Port, Database, User, JWT Token) and Flat File (upload, delimiter).
  - Column selection via checkboxes for targeted ingestion/export.
  - Real-time status updates (Connecting, Ingesting, Completed) and error alerts.
  - Responsive design with modern CSS for desktop and mobile.
- **Performance**:
  - Batch processing (1000 rows/batch) for fast ingestion.
  - Retry logic for transient ClickHouse failures.
  - Streamlined CSV/JSON parsing with `opencsv` and `jackson`.
- **Security**:
  - JWT token-based authentication for ClickHouse connections.
- **Reporting**:
  - Displays record counts post-ingestion (e.g., 10,297 CSV rows, 2 JSON rows).
- **Error Handling**:
  - Graceful recovery from malformed data, connection issues, and auth failures.
  - User-friendly error messages in the UI.
- **Extensibility**:
  - Modular code ready for formats like Parquet or parallel loading.

---

## 🛠️ Tech Stack

- **Backend**: Java 17, Spring Boot 3.3.5, ClickHouse JDBC 0.6.4
- **Frontend**: HTML, JavaScript, CSS (responsive, Inter font)
- **Libraries**:
  - `opencsv` 5.9 (CSV parsing)
  - `jackson-databind` 2.18.0 (JSON parsing)
  - `logback-classic` 1.5.8 (logging)
  - `commons-cli` 1.6.0 (CLI args, legacy)
- **Database**: ClickHouse (local Docker or cloud)
- **Build**: Maven 3.9.6
- **Deployment**: Runnable JAR, `http://localhost:8080`

---


## 🚀 Quick Start

### Prerequisites
- **Java 17+**: [Install](https://adoptium.net/)
- **Maven 3.9+**: [Install](https://maven.apache.org/install.html)
- **Docker**: [Install](https://www.docker.com/get-started/) (for local ClickHouse)
- **ClickHouse**: Local Docker or cloud instance

### Setup
**Clone the repo**:
   ```bash
   git clone https://github.com/debajitadhikary/clickhouse-flatfile-tool.git
   cd clickhouse-flatfile-tool