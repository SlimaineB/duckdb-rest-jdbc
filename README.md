# duckdb-rest-jdbc

This project contains two components:

- **duckdb-rest-driver**: A custom JDBC driver that allows making REST calls to a remote backend.
- **duckdb-rest-server**: The backend server that exposes the real DuckDB JDBC driver over a REST API.

## Features

- Execute SQL queries on DuckDB remotely via HTTP/REST.
- Compatible with tools and applications using JDBC.
- Easy to deploy and integrate into distributed architectures.

## Prerequisites

- Java 11 or higher
- Maven
- Docker (optional, for server deployment)

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/duckdb-rest-jdbc.git
   cd duckdb-rest-jdbc
   ```

2. Build the driver:
   ```bash
   cd duckdb-rest-driver
   mvn clean package
   ```
   The driver will be available at `target/duckdb-rest-driver-1.0.0.jar`.

## Running the backend

To start the backend locally, go to the `duckdb-rest-server` directory and run:
```bash
cd duckdb-rest-server
mvn spring-boot:run
```

The JDBC endpoint is available at: `jdbc:duckdb://localhost:8080?useEncryption=false`

## Multi-backend and Load Balancing

To test with multiple backends, use the provided Docker Compose setup.  
This will start two REST backends and an Nginx instance as a load balancer:
```bash
cd duckdb-rest-server
docker compose up --build
```

The JDBC endpoint through Nginx is available at: `jdbc:duckdb://localhost:80?useEncryption=false`

