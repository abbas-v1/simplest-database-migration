package dev.esafzay.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;

public class SimplestDatabaseMigration {

    private static final Logger log = LoggerFactory.getLogger(SimplestDatabaseMigration.class);

    private final DataSource dataSource;
    private final String resourcePath;

    /**
     * Creates a new SimplestDatabaseMigration instance
     * @param dataSource The dataSource to be used for getting the database connection
     * @param resourcePath The directory in src/main/resources that contains the migrations scripts
     */
    public SimplestDatabaseMigration(DataSource dataSource, String resourcePath) {
        this.dataSource = dataSource;
        this.resourcePath = resourcePath;
    }

    private static final String MIGRATE_KEYWORD = "__migrate_";
    private static final String ROLLBACK_KEYWORD = "__rollback_";

    /**
     * Apply the newly created migrations scripts in the resource directory or rollback the scripts that were previously
     * stored in the database.
     * @throws IOException if migration files are not read from the resource directory
     * @throws SQLException if previous migrations were not read from the database, or new ones were not applied.
     */
    public void migrate() throws IOException, SQLException {
        log.info("Run database migration/rollback if any...");
        Map<String, String> migrationFiles = getMigrationFiles();

        try (Connection connection = this.dataSource.getConnection()) {

            Map<String, MigrationLog> appliedMigrations = getAppliedMigrations(connection);
            List<MigrationLog> unAppliedMigrations = getUnAppliedMigrations(appliedMigrations, migrationFiles);

            if (!unAppliedMigrations.isEmpty()) { // migrate flow
                for (MigrationLog migration : unAppliedMigrations) {
                    log.info("Migrate database change : {}", migration.version());
                    applyMigration(connection, migration);
                }
                log.info("Database migration is completed.");

            } else {
                MigrationLog rollback = getRollback(appliedMigrations, migrationFiles);
                if (rollback != null) { // rollback flow
                    log.info("Rollback database change : {}", rollback.version());
                    applyMigration(connection, rollback);
                    log.info("Database rollback is completed.");
                }
            }
        }
    }

    Map<String, String> getMigrationFiles() throws IOException {
        URL resourceUrl = getClass().getClassLoader().getResource(resourcePath);
        if (resourceUrl == null) {
            log.error("Migrations directory not found : {}", resourcePath);
            throw new IllegalStateException("Migrations directory not found : " + resourcePath);
        }

        File resourceDir = null;
        try {
            resourceDir = new File(resourceUrl.toURI());
        } catch (URISyntaxException ex) {
            log.error("Could not get database migration files", ex);
            throw new IOException("Could not get database migration files", ex);
        }

        File[] files = resourceDir.listFiles();
        if (files == null || files.length == 0) {
            log.info("No migration files found");
            return Map.of();
        }

        Map<String, String> migrationFiles = new HashMap<>();
        for (var file : files) {
            if (file.getName().endsWith(".sql")) {
                String sqlScript = readFileContent(file).trim();
                migrationFiles.put(file.getName(), sqlScript);
            }
        }
        return migrationFiles;
    }

    String readFileContent(File file) {
        StringBuilder content = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append(System.lineSeparator());
            }
        } catch (IOException ex) {
            log.error("Failed to read contents of the file", ex);
            return null;
        }

        return content.toString();
    }

    Map<String, MigrationLog> getAppliedMigrations(Connection connection) throws SQLException {
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

    List<MigrationLog> getUnAppliedMigrations(Map<String, MigrationLog> appliedMigrations, Map<String, String> migrationFiles) {
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

    MigrationLog getRollback(Map<String, MigrationLog> appliedMigrations, Map<String, String> migrationFiles) {
        for (String appliedMigrationFileName : appliedMigrations.keySet()) {
            if (!migrationFiles.containsKey(appliedMigrationFileName)) {
                String version = appliedMigrationFileName.replace(MIGRATE_KEYWORD, ROLLBACK_KEYWORD);
                MigrationLog migrationLog = appliedMigrations.get(appliedMigrationFileName);
                return new MigrationLog(version, migrationLog.rollback(), "");
            }
        }
        return null;
    }

    void applyMigration(Connection connection, MigrationLog migration) throws SQLException {
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
