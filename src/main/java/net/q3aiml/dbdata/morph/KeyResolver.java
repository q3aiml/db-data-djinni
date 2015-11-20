package net.q3aiml.dbdata.morph;

import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.model.ForeignKeyReference;
import net.q3aiml.dbdata.model.Table;
import net.q3aiml.dbdata.model.TableToTableReference;
import net.q3aiml.dbdata.util.MoreCollectors;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

/**
 * Map between foreign keys and the referenced row {@link net.q3aiml.dbdata.DataSpec.DataSpecRow} in a {@link DataSpec}.
 */
public class KeyResolver {
    /**
     * Replace {@link net.q3aiml.dbdata.DataSpec.DataSpecRow} column values in {@code dataSpec} with key values
     */
    public void toKeys(DataSpec dataSpec, DatabaseMetadata db) {
        for (DataSpec.DataSpecRow row : dataSpec.tableRows) {
            for (Map.Entry<String, Object> rowColumn : row.getRow().entrySet()) {
                Object value = rowColumn.getValue();
                if (value instanceof DataSpec.DataSpecRow) {
                    Table table = db.tableByNameNoCreate(row.getTable());
                    Set<TableToTableReference> outgoingReferences = db.referredToBy(table);
                    Map<String, ForeignKeyReference> outgoingReferencesByColumn = outgoingReferences.stream()
                            .filter(ForeignKeyReference.class::isInstance)
                            .map(ForeignKeyReference.class::cast)
                            .collect(MoreCollectors.toMap(e -> e.getReferencingColumns().get(0), identity()));

                    ForeignKeyReference foreignKeyReference = outgoingReferencesByColumn.get(rowColumn.getKey());
                    if (foreignKeyReference == null) {
                        continue;
                    }

                    DataSpec.DataSpecRow referencedRow = (DataSpec.DataSpecRow)value;
                    rowColumn.setValue(referencedRow.getRow().get(foreignKeyReference.getReferencedColumns().get(0)));
                }
            }
        }
    }

    /**
     * Replace key values in {@code dataSpec} with referenced {@link net.q3aiml.dbdata.DataSpec.DataSpecRow}
     */
    public void toReferences(DataSpec dataSpec, DatabaseMetadata db) {
        Map<Table, Set<String>> referencedColumnsByTable = db.references().stream()
                .filter(ForeignKeyReference.class::isInstance)
                .map(ForeignKeyReference.class::cast)
                .collect(Collectors.groupingBy(ForeignKeyReference::getReferencedTable,
                        Collectors.collectingAndThen(
                                Collectors.mapping(ForeignKeyReference::getReferencedColumns, Collectors.toSet()),
                                e -> e.stream().flatMap(Collection::stream).collect(Collectors.toSet()))));

        Map<TableColumnValue, DataSpec.DataSpecRow> rowLookup = dataSpec.tableRows.stream()
                .flatMap(row -> {
                    Table table = db.tableByNameNoCreate(row.getTable());
                    Set<String> referencedColumns = referencedColumnsByTable.getOrDefault(table, Collections.emptySet());
                    return row.getRow().entrySet().stream()
                            .filter(columnValue -> referencedColumns.contains(columnValue.getKey()))
                            .map(value -> new AbstractMap.SimpleImmutableEntry<>(
                                    new TableColumnValue(table, value.getKey(), value.getValue()),
                                    row
                            ));
                })
                .collect(MoreCollectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        for (DataSpec.DataSpecRow row : dataSpec.tableRows) {
            Table table = db.tableByNameNoCreate(row.getTable());
            Set<TableToTableReference> outgoingReferences = db.referredToBy(table);
            Map<String, ForeignKeyReference> outgoingReferencesByColumn = outgoingReferences.stream()
                    .filter(ForeignKeyReference.class::isInstance)
                    .map(ForeignKeyReference.class::cast)
                    .collect(MoreCollectors.toMap(e -> e.getReferencingColumns().get(0), identity()));
            if (!(row.getRow() instanceof HashMap)) {
                row.setRow(new HashMap<>(row.getRow()));
            }
            for (Map.Entry<String, Object> rowColumn : row.getRow().entrySet()) {
                ForeignKeyReference reference = outgoingReferencesByColumn.get(rowColumn.getKey());
                if (reference != null) {
                    DataSpec.DataSpecRow referencedRow = rowLookup
                            .get(new TableColumnValue(reference.getReferencedTable(), reference.getReferencedColumns().get(0),
                                    rowColumn.getValue()));
                    if (referencedRow == null) {
                        continue;
                    }
                    rowColumn.setValue(referencedRow);
                }
            }
        }
    }

    protected static class TableColumnValue {
        public final Table table;
        public final String column;
        public final Object value;

        public TableColumnValue(Table table, String column, Object value) {
            this.table = table;
            this.column = column;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableColumnValue that = (TableColumnValue)o;
            return Objects.equals(table, that.table) &&
                    Objects.equals(column, that.column) &&
                    Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(table, column, value);
        }

        @Override
        public String toString() {
            return "TableColumnValue{" +
                    "table=" + table +
                    ", column='" + column + '\'' +
                    ", value=" + value +
                    '}';
        }
    }
}
