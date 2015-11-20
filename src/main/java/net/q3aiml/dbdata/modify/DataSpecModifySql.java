package net.q3aiml.dbdata.modify;

import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.jdbc.UnpreparedStatement;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.model.UniqueInfo;
import net.q3aiml.dbdata.util.MoreCollectors;

import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Generate sql to insert, delete, or update a {@link net.q3aiml.dbdata.DataSpec.DataSpecRow}.
 */
public class DataSpecModifySql {
    public UnpreparedStatement insertSql(DataSpec.DataSpecRow row, DatabaseMetadata db) {
        Map<String, Object> values = row.getRow();
        String columns = values.keySet().stream().collect(joining(", "));
        String valuePlaceholders = Collections.nCopies(values.size(), "?").stream().collect(joining(", "));
        String sql = "insert into " + row.getTable() + " (" + columns + ") values (" + valuePlaceholders + ")";
        return new UnpreparedStatement(sql, new ArrayList<>(values.values()));
    }

    public UnpreparedStatement deleteSql(DataSpec.DataSpecRow row, DatabaseMetadata db) {
        String tableName = row.getTable();
        Map<String, Object> columnValues = row.getRow();
        Set<String> uniqueColumnSet = uniqueColumnSet(row, db);
        String whereClause =  uniqueColumnSet.stream().map(column -> column + " = ?").collect(joining(" and "));
        String sql = "delete from " + tableName + " where " + whereClause;
        List<Object> values = columnValues.entrySet().stream()
                .filter(e -> uniqueColumnSet.contains(e.getKey()))
                .map(Map.Entry::getValue)
                .collect(toList());
        return new UnpreparedStatement(sql, values);
    }

    public UnpreparedStatement updateSql(
            DataSpec.DataSpecRow desiredRow, DataSpec.DataSpecRow currentRow, DatabaseMetadata db)
    {
        String tableName = desiredRow.getTable();
        Map<String, Object> columnValues = desiredRow.getRow();
        Map<String, Object> changedValues;
        if (currentRow == null) {
            changedValues = columnValues;
        } else {
            changedValues = columnValues.entrySet().stream()
                    .filter(desiredColumn -> !Objects.equals(desiredColumn.getValue(),
                            currentRow.getRow().get(desiredColumn.getKey())))
                    .collect(MoreCollectors.toMap(Map.Entry::getKey, Map.Entry::getValue, LinkedHashMap::new));
        }

        Set<String> uniqueColumnSet = uniqueColumnSet(desiredRow, db);
        String setClause =  changedValues.keySet().stream().map(column -> column + " = ?").collect(joining(" and "));
        String whereClause =  uniqueColumnSet.stream().map(column -> column + " = ?").collect(joining(" and "));
        String sql = "update " + tableName + " set " + setClause + " where " + whereClause;
        List<Object> values = Stream.concat(
                // set values
                changedValues.values().stream(),
                // where values
                columnValues.entrySet().stream()
                        .filter(e -> uniqueColumnSet.contains(e.getKey()))
                        .map(Map.Entry::getValue)
        ).collect(toList());
        return new UnpreparedStatement(sql, values);
    }

    protected Set<String> uniqueColumnSet(DataSpec.DataSpecRow row, DatabaseMetadata db) {
        Map<String, Object> columnValues = row.getRow();
        UniqueInfo uniqueInfo = db.tableByNameNoCreate(row.getTable()).uniqueInfo();

        Set<Set<String>> viableUniqueColumnSets = uniqueInfo.findAvailableUniqueColumnSets(columnValues.keySet());
        if (viableUniqueColumnSets.isEmpty()) {
            throw new IllegalArgumentException("cannot generate query: the set of column values does not "
                    + "uniquely identify rows in " + row.getTable() + " based on table primary keys and indexes."
                    + "\nUnique column sets: " + uniqueInfo.uniqueColumnSets()
                    + "\nData spec row: " + row);
        }

        // XXX just pick the first match for now. at least that way we prefer primary key
        return viableUniqueColumnSets.iterator().next();
    }
}
