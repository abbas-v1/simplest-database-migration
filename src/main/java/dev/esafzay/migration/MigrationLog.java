package dev.esafzay.migration;

class MigrationLog {

    private final String version;
    private final String migrate;
    private final String rollback;

    public MigrationLog(String version, String migrate, String rollback) {
        this.version = version;
        this.migrate = migrate;
        this.rollback = rollback;
    }

    public String getVersion() {
        return version;
    }

    public String getMigrate() {
        return migrate;
    }

    public String getRollback() {
        return rollback;
    }
}
