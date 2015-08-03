package net.q3aiml.dbdata.verify;

import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.config.DatabaseConfig;
import net.q3aiml.dbdata.introspect.DefaultDatabaseIntrospector;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.model.Table;
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

    public List<VerificationError> verify(DataSource dataSource, DataSpec dataSpec, DatabaseConfig config)
            throws SQLException
    {
        try (Connection c = dataSource.getConnection()) {
            return verify(c, dataSpec, config);
        }
    }

    public List<VerificationError> verify(Connection c, DataSpec dataSpec, DatabaseConfig config) throws SQLException {
        DatabaseMetadata db = new DatabaseMetadata();
        introspector.loadTables(c, db);
        introspector.loadTablePrimaryKeyInfo(c, db);
        introspector.loadTableUniqueInfo(c, db);
        introspector.loadReferences(c, db);
        return verify(c, dataSpec, config, db);
    }

    public List<VerificationError> verify(Connection c, DataSpec dataSpec, DatabaseConfig config, DatabaseMetadata db) throws SQLException {
        rekeyer.rekey(dataSpec, c, config.rekeyer, db);
        keyResolver.toKeys(dataSpec, db);

        List<VerificationError> errors = new ArrayList<>();
        for (DataSpec.DataSpecRow expectedRow : dataSpec.tableRows) {
            Table table = db.tableByNameNoCreate(expectedRow.getTable());
            Set<Set<String>> availableUniqueColumnSets = table.uniqueInfo()
                    .findAvailableUniqueColumnSets(expectedRow.getRow().keySet());
            Set<String> uniqueColumnSet = availableUniqueColumnSets.iterator().next();
            String where = uniqueColumnSet.stream()
                    .map(s -> s + " = ?")
                    .collect(Collectors.joining(" AND "));
            String sql = "select * from " + expectedRow.getTable() + " where " + where;
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                int columnIndex = 1;
                for (String column : uniqueColumnSet) {
                    String value = (String)expectedRow.getRow().get(column);
                    ps.setString(columnIndex++, value);
                }

                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        for (Map.Entry<String, Object> entry : expectedRow.getRow().entrySet()) {
                            String actualValue;
                            try {
                                actualValue = rs.getString(entry.getKey());
                            } catch (SQLException e) {
                                throw new SQLException("unable to get column " + entry.getKey() + " from "
                                        + expectedRow.getTable(), e);
                            }
                            if (!Objects.equals(actualValue, entry.getValue())) {
                                DataSpec.DataSpecRow actualRow =
                                        DataSpec.DataSpecRow.copyOfResultSetAsStrings(expectedRow.getTable(), rs);
                                errors.add(new VerificationError(VerificationError.Type.VALUE_MISMATCH, actualRow,
                                        expectedRow));
                                continue;
                            }
                        }
                    } else {
                        errors.add(new VerificationError(VerificationError.Type.MISSING_ROW, null, expectedRow));
                    }
                }
            }
        }
        return errors;
    }
}
