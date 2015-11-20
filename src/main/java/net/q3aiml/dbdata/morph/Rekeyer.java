package net.q3aiml.dbdata.morph;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.config.RekeyerConfig;
import net.q3aiml.dbdata.jdbc.UnpreparedStatement;
import net.q3aiml.dbdata.jdbc.UnpreparedStatementExecutor;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.model.ForeignKeyReference;
import net.q3aiml.dbdata.model.Table;
import net.q3aiml.dbdata.util.MoreCollectors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;

/**
 * Query database to lookup primary keys based on other unique sets of columns (alt keys).
 * <p/>
 * Handles cases where primary keys differ between environments and other columns need to be
 * used to determine when rows are functionally equivalent.
 */
public class Rekeyer {
    protected UnpreparedStatementExecutor executor = new UnpreparedStatementExecutor();

    public void rekey(DataSpec dataSpec, Connection c, RekeyerConfig config, DatabaseMetadata db)
            throws SQLException
    {
        for (DataSpec.DataSpecRow row : dataSpec.tableRows) {
            Table table = db.tableByNameNoCreate(row.getTable());
            List<AltKeyColumn> altKeyColumns = parseAltKey(table, config.altKeys.get(row.getTable()), db);
            if (!altKeyColumns.isEmpty()) {
                Map<String, Object> values = row.getRow();

                Map<AltKeyColumn, Optional<Object>> altKeyValues = altKeyColumns.stream()
                        .collect(MoreCollectors.toMap(identity(), altKey -> altKey.getValue(values, db),
                                LinkedHashMap::new));

                if (altKeyValues.values().stream().allMatch(Optional::isPresent)) {

                    UnpreparedStatement sql = altKeyQuery(row.getTable(), altKeyValues, db);

                    try (PreparedStatement ps = executor.prepare(c, sql)) {
                        try (ResultSet rs = ps.executeQuery()) {
                            applyResultSet(rs, db, table, values);
                        }
                    }
                }
            }
        }
    }

    protected List<AltKeyColumn> parseAltKey(Table table, List<String> altKey, DatabaseMetadata db) {
        if (altKey == null) {
            return Collections.emptyList();
        } else {
            return altKey.stream()
                    .map(altKeyColumn -> parseAltKeyColumn(table, altKeyColumn, db))
                    .collect(toList());
        }
    }

    /**
     * parses an alt key string (possibly a complex alt key involving a path) into an
     * {@link net.q3aiml.dbdata.morph.Rekeyer.AltKeyColumn}
     */
    protected AltKeyColumn parseAltKeyColumn(Table table, String altKeyColumnPathString, DatabaseMetadata db) {
        List<String> altKeyColumnPath = Splitter.on(".").splitToList(altKeyColumnPathString);
        List<ForeignKeyReference> referencePath = new ArrayList<>(altKeyColumnPath.size() - 1);
        Table nextReferencingTable = table;
        // skip last column as that should be a plain value rather than a reference
        for (String altKeyColumn : altKeyColumnPath.subList(0, altKeyColumnPath.size() - 1)) {
            List<ForeignKeyReference> refs = db.referredToBy(nextReferencingTable).stream()
                    .filter(ForeignKeyReference.class::isInstance)
                    .map(ForeignKeyReference.class::cast)
                    .filter(ref -> ref.getReferencingColumns().contains(altKeyColumn))
                    .collect(toList());
            if (refs.size() != 1) {
                throw new IllegalArgumentException("invalid alt key error: expected exactly one foreign key "
                        + "reference on " + nextReferencingTable + " via column " + altKeyColumn + ") "
                        + "but found " + refs.size() + ": " + refs);
            }
            ForeignKeyReference ref = refs.get(0);
            nextReferencingTable = ref.getReferencedTable();
            referencePath.add(ref);
        }
        return new AltKeyColumn(table, altKeyColumnPath, referencePath);
    }

    protected UnpreparedStatement altKeyQuery(
            String table, Map<AltKeyColumn, Optional<Object>> altKeyColumnValues, DatabaseMetadata db)
    {
        Set<ForeignKeyReference> joinRelationships = altKeyColumnValues.keySet().stream()
                .flatMap(altKeyColumn -> altKeyColumn.referencePath.stream())
                .collect(toCollection(LinkedHashSet::new));

        String joins = joinRelationships.stream().map(ref -> {
            String whereClause = IntStream.range(0, ref.getReferencedColumns().size())
                    .mapToObj(i -> ref.getReferencedTable().fullName() + "." + ref.getReferencedColumns().get(i)
                            + " = " + ref.getReferencingTable().fullName() + "." + ref.getReferencingColumns().get(i))
                    .collect(joining(" AND "));
            return "LEFT JOIN " + ref.getReferencedTable().fullName() + " ON (" + whereClause + ")";
        }).collect(joining(" AND "));

        String sql = "SELECT " + table + ".* FROM " + table + " ";
        if (!joins.isEmpty()) {
            sql += joins + " ";
        }
        sql += "WHERE " + altKeyColumnValues.keySet().stream()
                .map(e -> e.lastAltKeyColumnPartQualified() + " = ?")
                .collect(joining(" AND "));

        List<Object> values = new ArrayList<>();
        int i = 1;
        for (Map.Entry<AltKeyColumn, Optional<Object>> columnValue : altKeyColumnValues.entrySet()) {
            Object value = columnValue.getValue().get();
            values.add(value);
        }

        return new UnpreparedStatement(sql, values);
    }

    protected void applyResultSet(ResultSet rs, DatabaseMetadata db, Table table, Map<String, Object> applyTo)
            throws SQLException
    {
        if (rs.next()) {
            List<String> primaryKeyColumns = table.primaryKeyColumns();
            List<ForeignKeyReference> outgoingReferences = db.referredToBy(table).stream()
                    .filter(ForeignKeyReference.class::isInstance)
                    .map(ForeignKeyReference.class::cast)
                    .collect(toList());

            for (String primaryKeyColumn : primaryKeyColumns) {
                applyTo.put(primaryKeyColumn, rs.getString(primaryKeyColumn));
            }
            for (ForeignKeyReference outgoingReference : outgoingReferences) {
                for (int column = 0; column < outgoingReference.getReferencingColumns().size(); column++) {
                    String referencingColumn = outgoingReference.getReferencingColumns().get(column);
                    String referencedColumn = outgoingReference.getReferencedColumns().get(column);
                    Object referencingValue = applyTo.get(referencingColumn);
                    if (referencingValue instanceof DataSpec.DataSpecRow) {
                        DataSpec.DataSpecRow referencedRow = (DataSpec.DataSpecRow)referencingValue;
                        referencedRow.getRow().put(referencedColumn, rs.getString(referencingColumn));
                    }
                }
            }
        }
    }

    /**
     * parsed alt key and relationships involved
     */
    protected static class AltKeyColumn {
        protected Table startTable;
        protected List<String> altKeyColumnPath;
        protected List<ForeignKeyReference> referencePath;

        public AltKeyColumn(Table startTable, List<String> altKeyColumnPath, List<ForeignKeyReference> referencePath) {
            checkArgument(referencePath.size() == altKeyColumnPath.size() - 1, "referencePath length (%s) must be "
                    + "one less than altKeyColumnPath (%s)", referencePath.size(), altKeyColumnPath.size());
            this.startTable = startTable;
            this.altKeyColumnPath = altKeyColumnPath;
            this.referencePath = referencePath;
        }

        public String lastAltKeyColumnPartQualified() {
            String column = altKeyColumnPath.get(altKeyColumnPath.size() - 1);
            if (!referencePath.isEmpty()) {
                ForeignKeyReference reference = referencePath.get(referencePath.size() - 1);
                column = reference.getReferencedTable().fullName() + "." + column;
            } else {
                column = startTable.fullName() + "." + column;
            }
            return column;
        }

        /**
         * Find the value referred to by this alt key in the row values {@code values}.
         * @param values a single row's values as a map of the column name to value
         */
        public Optional<Object> getValue(Map<String, Object> values, DatabaseMetadata db) {
            Object value = values.get(altKeyColumnPath.get(0));
            for (String altKeyPart : Iterables.skip(altKeyColumnPath, 1)) {
                if (value instanceof DataSpec.DataSpecRow) {
                    DataSpec.DataSpecRow valueRow = (DataSpec.DataSpecRow) value;
                    value = valueRow.getRow().get(altKeyPart);
                }
            }

            if (value instanceof DataSpec.DataSpecRow) {
                DataSpec.DataSpecRow valueRow = (DataSpec.DataSpecRow)value;
                Table referencingTable;
                if (referencePath.isEmpty()) {
                    referencingTable = startTable;
                } else {
                    referencingTable = referencePath.get(referencePath.size() - 1)
                            .getReferencedTable();
                }
                String referencingColumn = altKeyColumnPath.get(altKeyColumnPath.size() - 1);
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

            return Optional.ofNullable(value);
        }

        @Override
        public String toString() {
            return "AltKeyColumn{" +
                    "altKeyColumnPath=" + altKeyColumnPath +
                    ", referencePath=" + referencePath +
                    '}';
        }
    }
}
