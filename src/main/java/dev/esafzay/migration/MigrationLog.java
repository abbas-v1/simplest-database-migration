package dev.esafzay.migration;

record MigrationLog(String version, String migrate, String rollback) {
}
