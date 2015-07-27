package net.q3aiml.dbdata.export;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.config.DatabaseConfig;
import net.q3aiml.dbdata.introspect.DefaultDatabaseIntrospector;
import net.q3aiml.dbdata.model.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;

public class Exporter {
    protected final DataSource dataSource;
    protected final DatabaseConfig config;
    protected final DefaultDatabaseIntrospector introspector;
    protected DatabaseMetadata db;
    private boolean noTableReferenceLoops = true;

    public Exporter(DataSource dataSource, DatabaseConfig config) {
        this.dataSource = dataSource;
        this.config = config;
        introspector = new DefaultDatabaseIntrospector(config.schema);
    }

    public String extractAndSerialize(String startTable, String startQuery)
            throws JsonProcessingException, SQLException
    {
        List<TableData> allDatas = extract(startTable, startQuery);
        DataSpec dataSpec = serialize(allDatas, config);
        return dataSpec.toYaml(config);
    }

    public List<TableData> extract(String startTableName, String startQuery) throws SQLException {
        List<TableData> allDatas = new ArrayList<>();
        try (Connection c = dataSource.getConnection()) {
            db = new DatabaseMetadata();
            introspector.load(c, db);

            Table startTable = db.tableByNameNoCreate(startTableName);
            TableData startData = executeStart(c, startTable, startQuery);

            Deque<TableData> tableDatas = new ArrayDeque<>();
            tableDatas.addLast(startData);
            Set<String> visited = new HashSet<>();
            visited.add(startData.table.schema + "." + startData.table.name);
            while (!tableDatas.isEmpty()) {
                TableData tableData = tableDatas.removeFirst();
                allDatas.add(tableData);

                List<TableData> newTableDatas = extractFollowReferences(c, db, tableData, visited);
                tableDatas.addAll(newTableDatas);
            }
        }

        return allDatas;
    }

    protected TableData executeStart(Connection c, Table table, String startQuery) throws SQLException {
        String sql = "SELECT * FROM " + table.schema + "." + table.name + " " + startQuery;
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                return TableData.copyOfResultSet(table, rs);
            }
        }
    }

    public DataSpec serialize(List<TableData> allDatas, DatabaseConfig databaseConfig) throws JsonProcessingException {
        DataSpec dataSpec = new DataSpec();
        for (TableData data : allDatas) {
            for (int row = 0; row < data.rowCount(); row++) {
                DataSpec.DataSpecRow magicRow = new DataSpec.DataSpecRow();
                magicRow.setTable(data.table.schema + "." + data.table.name);

                for (int column = 0; column < data.columnNames.length; column++) {
                    String columnName = data.columnNames[column];
                    String value = data.data(row, column);

                    if (!databaseConfig.isIgnored(columnName)) {
                        magicRow.getRow().put(columnName, value);
                    }
                }

                dataSpec.tableRows.add(magicRow);
            }
        }

        return dataSpec;
    }

    public List<TableData> extractFollowReferences(Connection c, DatabaseMetadata db, TableData tableData, Set<String> visited) throws SQLException {
        List<TableData> foundData = new ArrayList<>();
        for (TableToTableReference reference : db.references(tableData.table)) {
            if (reference instanceof ForeignKeyReference) {
                ForeignKeyReference fk = (ForeignKeyReference)reference;
                Table otherTable;
                List<String> otherColumns;
                List<String> ourColumns;
                if (fk.getReferencedTable() == tableData.table) {
                    ourColumns = fk.getReferencedColumns();
                    otherTable = fk.getReferencingTable();
                    otherColumns = fk.getReferencingColumns();
                } else {
                    ourColumns = fk.getReferencingColumns();
                    otherTable = fk.getReferencedTable();
                    otherColumns = fk.getReferencedColumns();
                }

                if (noTableReferenceLoops) {
                    String visitKey = otherTable.schema + "." + otherTable.name;
                    if (visited.contains(visitKey)) {
                        continue;
                    }
                    visited.add(visitKey);
                }

                String sql = "SELECT * FROM " + otherTable.schema + "." + otherTable.name + " WHERE "
                        + otherColumns.stream().map(col -> col + " = ?").collect(joining(" AND "));
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    for (int i = 0; i < tableData.rowCount(); i++) {
                        int iRow = i;
                        String visitKeyData = IntStream.range(0, otherColumns.size())
                                .mapToObj(iCol -> otherColumns.get(iCol) + "=" + tableData.data(iRow, ourColumns.get(iCol)))
                                .collect(joining(","));
                        String visitKey = otherTable.schema + "." + otherTable.name + "." + visitKeyData;

                        if (!visited.contains(visitKey)) {
                            visited.add(visitKey);
                            int iCol = 1;
                            for (String ourColumn : ourColumns) {
                                ps.setString(iCol++, tableData.data(i, ourColumn));
                            }
                            try (ResultSet rs = ps.executeQuery()) {
                                TableData tableData1 = TableData.copyOfResultSet(otherTable, rs);
                                foundData.add(tableData1);
                            }
                        }
                    }
                }
            } else {
                throw new UnsupportedOperationException("unsupported reference " + reference);
            }
        }

        return foundData;
    }
}
