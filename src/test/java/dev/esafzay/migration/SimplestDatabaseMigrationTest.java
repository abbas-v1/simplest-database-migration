package dev.esafzay.migration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import javax.sql.DataSource;
import java.nio.file.*;
import java.sql.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SimplestDatabaseMigrationTest {

    @Mock
    private DataSource dataSource;
    @Mock
    private Connection connection;
    @Mock
    private Statement statement;
    @Mock
    private Statement statement2;
    @Mock
    private PreparedStatement preparedStatement;
    @Mock
    private ResultSet resultSet;

    @BeforeEach
    void setUp() throws Exception {
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement, statement2);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testMigrate_NoMigrations_NoRollbacks() throws Exception {
        var resourcePath = "migrations/no-migrations";
        var simplestDatabaseMigration = new SimplestDatabaseMigration(dataSource, resourcePath);
        when(resultSet.next()).thenReturn(false);

        simplestDatabaseMigration.migrate();

        verify(connection, never()).setAutoCommit(false);
        verify(statement, never()).execute(anyString());
        verify(preparedStatement, never()).executeUpdate();
        verify(connection, never()).commit();
    }

    @Test
    void testMigrate_OneMigrations_NoRollbacks() throws Exception {
        var resourcePath = "migrations/one-migrations";
        var simplestDatabaseMigration = new SimplestDatabaseMigration(dataSource, resourcePath);
        when(resultSet.next()).thenReturn(false);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

        simplestDatabaseMigration.migrate();

        verify(connection).setAutoCommit(false);
        verify(statement2).execute("CREATE TABLE test (id INT);");
        verify(preparedStatement).setString(1, "20230801__migrate_create_table.sql");
        verify(preparedStatement).setString(2, "CREATE TABLE test (id INT);");
        verify(preparedStatement).setString(3, null);
        verify(preparedStatement).executeUpdate();
        verify(connection).commit();
    }

    @Test
    void testMigrate_TwoMigrations_NoRollbacks() throws Exception {
        var resourcePath = "migrations/two-migrations";
        var simplestDatabaseMigration = new SimplestDatabaseMigration(dataSource, resourcePath);
        when(resultSet.next()).thenReturn(false);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

        simplestDatabaseMigration.migrate();

        verify(connection, times(2)).setAutoCommit(false);
        verify(statement2).execute("CREATE TABLE another (id INT);");
        verify(preparedStatement).setString(1, "20230801__migrate_create_table.sql");
        verify(preparedStatement).setString(2, "CREATE TABLE another (id INT);");
        verify(preparedStatement).setString(3, "DROP TABLE another;");
        verify(preparedStatement, times(2)).executeUpdate();
        verify(connection, times(2)).commit();
    }

}



