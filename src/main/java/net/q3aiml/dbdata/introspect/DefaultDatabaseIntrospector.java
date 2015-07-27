package net.q3aiml.dbdata.introspect;

import net.q3aiml.dbdata.jdbc.ResultSets;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.model.ForeignKeyReference;
import net.q3aiml.dbdata.model.Table;
import org.jooq.lambda.SQL;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultDatabaseIntrospector {
    private final String desiredSchema;

    public DefaultDatabaseIntrospector(String schema) {
        this.desiredSchema = schema;
    }

    public void load(Connection connection, DatabaseMetadata db) throws SQLException {
        loadTables(connection, db);
        loadReferences(connection, db);
    }

    public void loadTables(Connection connection, DatabaseMetadata db) throws SQLException {
        try (ResultSet tables = connection.getMetaData().getTables(null, desiredSchema, null, null)) {
            while (tables.next()) {
                String schema = tables.getString("TABLE_SCHEM");
                String tableName = tables.getString("TABLE_NAME");
                db.addTable(schema, tableName);
            }
        }
    }

    public void loadReferences(Connection connection, DatabaseMetadata db) throws SQLException {
        try (ResultSet references = connection.getMetaData().getCrossReference(null, null, null, null, desiredSchema, null)) {
            // already ordered by KEY_SEQ
            SQL.seq(references, ResultSets.toMap(references.getMetaData()))
                    .groupBy(row -> row.get("FKTABLE_SCHEM") + "." + row.get("FKTABLE_NAME") + "." + row.get("FK_NAME"))
                    .values()
                    .forEach(rows -> {
                        Map<String, String> firstRow = rows.get(0);
                        String primaryKeySchema = firstRow.get("PKTABLE_SCHEM");
                        String primaryKeyTableName = firstRow.get("PKTABLE_NAME");
                        String foreignKeySchema = firstRow.get("FKTABLE_SCHEM");
                        String foreignKeyTableName = firstRow.get("FKTABLE_NAME");
                        Table primaryKeyTable = db.table(primaryKeySchema, primaryKeyTableName);
                        Table foreignKeyTable = db.table(foreignKeySchema, foreignKeyTableName);
                        List<String> primaryKeyColumns = rows.stream().map(row -> row.get("PKCOLUMN_NAME"))
                                .collect(Collectors.toList());
                        List<String> foreignKeyColumns = rows.stream().map(row -> row.get("FKCOLUMN_NAME"))
                                .collect(Collectors.toList());
                        ForeignKeyReference reference = new ForeignKeyReference(
                                primaryKeyTable, primaryKeyColumns,
                                foreignKeyTable, foreignKeyColumns);
                        if (primaryKeyColumns.size() == 1) {
                            db.addReference(foreignKeyTable, primaryKeyTable, reference);
                        }
                    });
        }
    }
}