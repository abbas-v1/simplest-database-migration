package dev.esafzay.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

public class SimplestDatabaseMigration {

    private static final Logger log = LoggerFactory.getLogger(SimplestDatabaseMigration.class);

    private final DataSource dataSource;
    private final Path resourcePath;

    public SimplestDatabaseMigration(DataSource dataSource, Path resourcePath) {
        this.dataSource = dataSource;
        this.resourcePath = resourcePath;
    }

    private static final String MIGRATE_KEYWORD = "__migrate_";
    private static final String ROLLBACK_KEYWORD = "__rollback_";

    public void migrate() throws IOException, SQLException {

        log.info("Run database migration/rollback if any...");
        Map<String, String> migrationFiles = getMigrationFiles();

        try (Connection connection = this.dataSource.getConnection()) {

            Map<String, MigrationLog> appliedMigrations = getAppliedMigrations(connection);
            List<MigrationLog> unAppliedMigrations = getUnAppliedMigrations(appliedMigrations, migrationFiles);

            if (!unAppliedMigrations.isEmpty()) {
                for (MigrationLog migration : unAppliedMigrations) {
                    log.info("Migrate database change : {}", migration.version());
                    applyMigration(connection, migration);
                }
                log.info("Database migration is completed.");

            } else {
                MigrationLog rollback = getRollback(appliedMigrations, migrationFiles);
                if (rollback != null) {
                    log.info("Rollback database change : {}", rollback.version());
                    applyMigration(connection, rollback);
                    log.info("Database rollback is completed.");
                }
            }
        }
    }

    private Map<String, String> getMigrationFiles() throws IOException {
        Map<String, String> migrationFiles = new HashMap<>();
        try (Stream<Path> files = Files.list(this.resourcePath)) {
            files.filter(filePath -> filePath.toString().endsWith(".sql"))
                    .forEach(filePath -> {
                        try {
                            String sqlScript = Files.readString(filePath, StandardCharsets.UTF_8);
                            migrationFiles.put(filePath.getFileName().toString(), sqlScript);
                        } catch (IOException e) {
                            log.error("Could not read SQL script file: {}", filePath, e);
                        }
                    });
        } catch (Exception ex) {
            log.error("Could not get database migration files", ex);
            throw new IOException("Could not get database migration files", ex);
        }
        return migrationFiles;
    }

    private Map<String, MigrationLog> getAppliedMigrations(Connection connection) throws SQLException {
        Map<String, MigrationLog> appliedMigrations = new HashMap<>();
        String sql = "SELECT version, migrate_sql, rollback_sql FROM database_migration_log ORDER BY id";

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {

            while (resultSet.next()) {
                String version = resultSet.getString("version");
                if (version.contains(ROLLBACK_KEYWORD)) {
                    appliedMigrations.remove(version.replace(ROLLBACK_KEYWORD, MIGRATE_KEYWORD));
                } else {
                    appliedMigrations.put(version, new MigrationLog(version,
                            resultSet.getString("migrate_sql"),
                            resultSet.getString("rollback_sql")));
                }
            }
        }
        return appliedMigrations;
    }

    private List<MigrationLog> getUnAppliedMigrations(Map<String, MigrationLog> appliedMigrations,
                                                      Map<String, String> migrationFiles) {
        return migrationFiles.keySet().stream()
                .filter(migrationFileName -> migrationFileName.contains(MIGRATE_KEYWORD))
                .filter(migrationFileName -> !appliedMigrations.containsKey(migrationFileName))
                .map(migrationFileName -> {
                    String migrationScript = migrationFiles.get(migrationFileName);
                    String rollbackFileName = migrationFileName.replace(MIGRATE_KEYWORD, ROLLBACK_KEYWORD);
                    String rollbackScript = migrationFiles.get(rollbackFileName);
                    return new MigrationLog(migrationFileName, migrationScript, rollbackScript);
                }).toList();
    }

    private MigrationLog getRollback(Map<String, MigrationLog> appliedMigrations, Map<String, String> migrationFiles) {
        for (String appliedMigrationFileName : appliedMigrations.keySet()) {
            if (!migrationFiles.containsKey(appliedMigrationFileName)) {
                String version = appliedMigrationFileName.replace(MIGRATE_KEYWORD, ROLLBACK_KEYWORD);
                MigrationLog migrationLog = appliedMigrations.get(appliedMigrationFileName);
                return new MigrationLog(version, migrationLog.rollback(), "");
            }
        }
        return null;
    }

    private void applyMigration(Connection connection, MigrationLog migration) throws SQLException {
        log.info(migration.migrate());
        connection.setAutoCommit(false);
        String logSql = "INSERT INTO database_migration_log (version, migrate_sql, rollback_sql) VALUES(?, ?, ?)";

        try (Statement statement = connection.createStatement()) {
            statement.execute(migration.migrate());

            try (PreparedStatement preparedStatement = connection.prepareStatement(logSql)) {
                preparedStatement.setString(1, migration.version());
                preparedStatement.setString(2, migration.migrate());
                preparedStatement.setString(3, migration.rollback());
                preparedStatement.executeUpdate();
            }
            connection.commit();

        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }
}
