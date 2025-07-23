# 🕵️‍♂️ ChgMon - Database Change Monitor

> *"Who needs timestamps when you've got checksums?"* 

## 📋 Overview

ChgMon is a lightweight Java application that monitors changes in database tables, particularly useful for tables that don't have timestamp columns. It's like having a security camera for your database, but without the creepy feeling of being watched!

Using clever checksum calculations, ChgMon can detect when rows are inserted, updated, or deleted from a monitored table, creating a comprehensive audit trail of all changes.

## ✨ Features

- 🔍 Monitors changes in database tables **without requiring timestamp columns**
- 🚨 Detects INSERT, UPDATE, and DELETE operations
- 🧮 Uses efficient CRC32 checksums to identify changes
- 📝 Records all changes with timestamps in an audit table
- ⏱️ Configurable monitoring schedule via cron expressions
- 🎯 Configurable target table and primary key column
- 💾 Works with SQL Server databases
- 🚀 Built with Spring Boot for easy deployment

## 🧩 How It Works

ChgMon uses a clever approach to detect changes in database tables:

1. **Periodic Scanning**: The application periodically scans the configured database table according to a schedule.
2. **Checksum Calculation**: For each row in the table, it calculates a CRC32 checksum based on the row's content.
3. **Change Detection**:
   - **INSERT**: If a row exists but no checksum is found, it's a new row.
   - **UPDATE**: If a row's current checksum differs from its stored checksum, it's been modified.
   - **DELETE**: If a checksum exists but the corresponding row doesn't, it's been deleted.
4. **Audit Trail**: All detected changes are recorded in an audit table with timestamps.

Think of it as a "spot the difference" game, but for your database, and ChgMon never gets tired of playing!

## 🛠️ Installation

### Prerequisites

- Java 17 or higher
- Maven
- Microsoft SQL Server database

### Steps

1. Clone the repository:
   ```bash
   git clone https://your-repository-url/chgmon.git
   cd chgmon
   ```

2. Build the application:
   ```bash
   mvn clean package
   ```

3. Create the required database tables:
   - Run the SQL script in `src/main/resources/db/tables.sql`

4. Configure the application (see Configuration section below)

5. Run the application:
   ```bash
   java -jar target/chgmon-0.0.1-SNAPSHOT.jar
   ```

## ⚙️ Configuration

ChgMon is configured using environment variables and application properties:

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| JDBC_DATABASE_HOST | Database server hostname | localhost |
| JDBC_DATABASE_DB | Database name | mydb |
| JDBC_DATABASE_USERNAME | Database username | dbuser |
| JDBC_DATABASE_PASSWORD | Database password | dbpass |
| LOG_FILE_PATH | Log file directory (optional) | logs |

### Application Properties

The following properties can be configured in `application.properties`:

| Property | Description | Default |
|----------|-------------|---------|
| monitor.table-name | Name of the table to monitor | tb_fx_trade |
| monitor.primary-key-name | Name of the primary key column | id_fx_trade |
| monitor.cron | Monitoring schedule (cron expression) | 0 */2 7-21 * * 1-5 |

## 📊 Usage Examples

### Basic Usage

With default configuration, ChgMon will monitor the `tb_fx_trade` table every 2 minutes from 7 AM to 9 PM on weekdays.

### Custom Table Monitoring

To monitor a different table:

1. Update the `monitor.table-name` and `monitor.primary-key-name` properties
2. Restart the application

### Custom Schedule

To change the monitoring schedule:

1. Update the `monitor.cron` property with a valid cron expression
2. Restart the application

Example for monitoring every 5 minutes, 24/7:
```
monitor.cron=0 */5 * * * *
```

## 📝 Logging

ChgMon logs all detected changes at INFO level and provides detailed debugging information at DEBUG level.

Logs are stored in the file specified by the `LOG_FILE_PATH` environment variable (defaults to `logs/chgmon.log`).

## 🤝 Contributing

Contributions are welcome! Here's how you can contribute:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit your changes: `git commit -m 'Add amazing feature'`
4. Push to the branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

## 📄 License

This project is licensed under the GPLv3 License - see the LICENSE file for details.

## 🧠 Fun Facts

- The average database table experiences more changes in a day than a teenager's mood.
- If your database had feelings, it would appreciate ChgMon keeping track of all the changes it goes through.
- ChgMon can detect changes faster than you can say "WHERE IS MY TIMESTAMP COLUMN?!"

---

*Made with ❤️ by developers who got tired of asking "What changed in that table?"*