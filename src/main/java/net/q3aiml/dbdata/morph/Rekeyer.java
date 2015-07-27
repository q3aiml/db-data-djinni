package net.q3aiml.dbdata.morph;

import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.config.RekeyerConfig;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.model.Table;
import net.q3aiml.dbdata.util.MoreCollectors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;

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
                            .collect(MoreCollectors.toMap(identity(), values::get));
                    altKeyValues = new LinkedHashMap<>(altKeyValues);

                    String sql = "SELECT * FROM " + row.getTable() + " WHERE "
                            + altKeyValues.keySet().stream().map(e -> e + " = ?").collect(joining(" AND "));

                    try (PreparedStatement ps = c.prepareStatement(sql)) {
                        int i = 1;
                        for (Object value : altKeyValues.values()) {
                            ps.setObject(i++, value);
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
