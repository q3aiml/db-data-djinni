package net.q3aiml.dbdata.morph;

import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.config.RekeyerConfig;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.model.ForeignKeyReference;
import net.q3aiml.dbdata.model.Table;
import net.q3aiml.dbdata.util.MoreCollectors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Lookup primary keys based on other unique sets of columns
 */
public class Rekeyer {
    public void rekey(DataSpec dataSpec, Connection c, RekeyerConfig config, DatabaseMetadata db)
            throws SQLException
    {
        for (DataSpec.DataSpecRow row : dataSpec.tableRows) {
            List<String> altKeys = config.altKeys.get(row.getTable());
            if (altKeys != null && !altKeys.isEmpty()) {
                Table table = db.tableByNameNoCreate(row.getTable());

                List<String> primaryKeyColumns = table.primaryKeyColumns();

                Map<String, Object> values = row.getRow();

                if (altKeys.stream().allMatch(values.keySet()::contains)) {
                    if (altKeys.isEmpty()) {
                        continue;
                    }

                    Map<String, Object> altKeyValues = altKeys.stream()
                            .collect(MoreCollectors.toMap(identity(), values::get, LinkedHashMap::new));
                    altKeyValues = new LinkedHashMap<>(altKeyValues);

                    String sql = "SELECT * FROM " + row.getTable() + " WHERE "
                            + altKeyValues.keySet().stream().map(e -> e + " = ?").collect(joining(" AND "));

                    try (PreparedStatement ps = c.prepareStatement(sql)) {
                        int i = 1;
                        for (Map.Entry<String, Object> columnValue : altKeyValues.entrySet()) {
                            Object value = columnValue.getValue();
                            if (value instanceof DataSpec.DataSpecRow) {
                                DataSpec.DataSpecRow valueRow = (DataSpec.DataSpecRow)value;
                                Table referencingTable = db.tableByNameNoCreate(row.getTable());
                                String referencingColumn = columnValue.getKey();
                                Table referencedTable = db.tableByNameNoCreate(valueRow.getTable());

                                Set<ForeignKeyReference> references = db.referredToBy(referencingTable).stream()
                                        .filter(ForeignKeyReference.class::isInstance)
                                        .map(ForeignKeyReference.class::cast)
                                        .filter(ref -> ref.getReferencingColumns().contains(referencingColumn)
                                                && ref.getReferencedTable() == referencedTable)
                                        .collect(toSet());

                                if (!references.isEmpty()) {
                                    ForeignKeyReference reference = references.iterator().next();
                                    int column = reference.getReferencingColumns().indexOf(referencingColumn);
                                    String referencedColumn = reference.getReferencedColumns().get(column);
                                    value = valueRow.getRow().get(referencedColumn);
                                }
                            }

                            try {
                                ps.setObject(i++, value);
                            } catch (SQLException e) {
                                throw new SQLException("failed to setObject " + value + "\n\tsql: " + sql, e);
                            }
                        }

                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                for (String primaryKeyColumn : primaryKeyColumns) {
                                    values.put(primaryKeyColumn, rs.getString(primaryKeyColumn));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
