package net.q3aiml.dbdata.verify;

import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.config.DatabaseConfig;
import net.q3aiml.dbdata.introspect.DefaultDatabaseIntrospector;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.morph.KeyResolver;
import net.q3aiml.dbdata.morph.Rekeyer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class Verifier {
    private DefaultDatabaseIntrospector introspector;
    private Rekeyer rekeyer = new Rekeyer();
    private KeyResolver keyResolver = new KeyResolver();

    public Verifier(DefaultDatabaseIntrospector introspector) {
        this.introspector = introspector;
    }

    public void verify(DataSource dataSource, DataSpec magic, DatabaseConfig config) throws SQLException {
        try (Connection c = dataSource.getConnection()) {
            DatabaseMetadata db = new DatabaseMetadata();
            introspector.loadTables(c, db);
            introspector.loadTablePrimaryKeyInfo(c, db);
            introspector.loadTableUniqueInfo(c, db);
            introspector.loadReferences(c, db);

            rekeyer.rekey(magic, c, config.rekeyer, db);
            keyResolver.toKeys(magic, db);

            verify(c, magic, db);
        }
    }

    public void verify(Connection c, DataSpec magic, DatabaseMetadata db) throws SQLException {
        List<VerificationError> errors = new ArrayList<>();

        for (DataSpec.DataSpecRow row : magic.tableRows) {
            Set<Set<String>> availableUniqueColumnSets = db.tableByNameNoCreate(row.getTable()).uniqueInfo()
                    .findAvailableUniqueColumnSets(row.getRow().keySet());
            Set<String> uniqueColumnSet = availableUniqueColumnSets.iterator().next();
            String where = uniqueColumnSet.stream()
                    .map(s -> s + " = ?")
                    .collect(Collectors.joining(" AND "));
            String sql = "select * from " + row.getTable() + " where " + where;
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                int columnIndex = 1;
                for (String column : uniqueColumnSet) {
                    String value = (String)row.getRow().get(column);
                    ps.setString(columnIndex++, value);
                }

                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        for (Map.Entry<String, Object> entry : row.getRow().entrySet()) {
                            String actualValue = rs.getString(entry.getKey());
                            if (!Objects.equals(actualValue, entry.getValue())) {
                                errors.add(new VerificationError(VerificationError.Type.VALUE_MISMATCH, row, entry.getKey()));
                            }
                        }
                    } else {
                        errors.add(new VerificationError(VerificationError.Type.MISSING_ROW, row, null));
                    }
                }
            }
        }
        if (errors.isEmpty()) {
            System.out.println("verification passed");
        } else {
            System.out.println("verification failed " + errors);
        }
    }

    private static class VerificationError {
        String message;
        public VerificationError(Type type, DataSpec.DataSpecRow row, String column) {
            if (type == Type.MISSING_ROW) {
                message = "missing row " + row;
            } else {
                message = "mismatch on " + column + " in " + row;
            }
        }

        public enum Type {
            MISSING_ROW,
            VALUE_MISMATCH
        }

        @Override
        public String toString() {
            return message;
        }
    }
}
