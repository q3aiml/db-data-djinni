package net.q3aiml.dbdata.export;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.config.DatabaseConfig;
import net.q3aiml.dbdata.introspect.DefaultDatabaseIntrospector;
import net.q3aiml.dbdata.jdbc.TableDataQueryer;
import net.q3aiml.dbdata.jdbc.UnpreparedStatement;
import net.q3aiml.dbdata.model.*;
import net.q3aiml.dbdata.morph.KeyResolver;
import net.q3aiml.dbdata.morph.PrimaryKeyFilter;
import net.q3aiml.dbdata.morph.SortDataSpec;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Exporter {
    protected final DataSource dataSource;
    protected final DatabaseConfig config;
    protected final DefaultDatabaseIntrospector introspector;
    protected final ReferencedDataLookup referencedDataLookup;
    protected final TableDataQueryer tableDataQueryer;
    protected DatabaseMetadata db;
    private boolean noTableReferenceLoops = true;

    public Exporter(DataSource dataSource, DatabaseConfig config) {
        this(dataSource, config, new DefaultDatabaseIntrospector(config.schema), new ReferencedDataLookup(),
                new TableDataQueryer());
    }

    public Exporter(DataSource dataSource, DatabaseConfig config, DefaultDatabaseIntrospector introspector,
                    ReferencedDataLookup referencedDataLookup, TableDataQueryer tableDataQueryer)
    {
        this.dataSource = dataSource;
        this.config = config;
        this.introspector = introspector;
        this.referencedDataLookup = referencedDataLookup;
        this.tableDataQueryer = tableDataQueryer;
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
            visited.add(startData.table().schema + "." + startData.table().name);
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

    protected DataSpec toDataSpec(Iterable<TableData> tableDatas, DatabaseConfig databaseConfig) {
        DataSpec dataSpec = new DataSpec();
        for (TableData data : tableDatas) {
            for (int row = 0; row < data.rowCount(); row++) {
                DataSpec.DataSpecRow dataSpecRow = new DataSpec.DataSpecRow();
                dataSpecRow.setTable(data.table().schema + "." + data.table().name);

                for (int column = 0; column < data.columnNames.length; column++) {
                    String columnName = data.columnNames[column];
                    String value = data.data(row, column);

                    if (!databaseConfig.isIgnored(data.table(), columnName)) {
                        dataSpecRow.getRow().put(columnName, value);
                    }
                }

                dataSpec.tableRows.add(dataSpecRow);
            }
        }
        return dataSpec;
    }

    public DataSpec serialize(List<TableData> allDatas, DatabaseConfig databaseConfig) throws JsonProcessingException {
        DataSpec dataSpec = toDataSpec(allDatas, databaseConfig);

        KeyResolver keyResolver = new KeyResolver();
        keyResolver.toReferences(dataSpec, db);
        SortDataSpec sortDataSpec = new SortDataSpec();
        sortDataSpec.sortRowTables(dataSpec, db);
        PrimaryKeyFilter primaryKeyFilter = new PrimaryKeyFilter();
        primaryKeyFilter.filterPrimaryKeys(dataSpec, db);

        return dataSpec;
    }

    public List<TableData> extractFollowReferences(Connection c, DatabaseMetadata db, TableData tableData,
                                                   Set<String> visited)
            throws SQLException
    {
        List<TableData> foundData = new ArrayList<>();
        for (TableToTableReference reference : db.references(tableData.table())) {
            if (reference instanceof ForeignKeyReference) {
                ForeignKeyReference fk = (ForeignKeyReference)reference;
                ReferencedDataLookupInfo lookupInfo = new ReferencedDataLookupInfo(fk, tableData.table());

                if (noTableReferenceLoops) {
                    String visitKey = lookupInfo.otherTable.schema + "." + lookupInfo.otherTable.name;
                    if (visited.contains(visitKey)) {
                        continue;
                    }
                    visited.add(visitKey);
                }

                for (int row = 0; row < tableData.rowCount(); row++) {
                    UnpreparedStatement query = referencedDataLookup.query(lookupInfo, tableData, row);
                    String visitKey = query.toString();

                    if (!visited.contains(visitKey)) {
                        visited.add(visitKey);
                        TableData lookedUpData = tableDataQueryer.execute(c, query, lookupInfo.otherTable);
                        foundData.add(lookedUpData);
                    }
                }
            } else {
                throw new UnsupportedOperationException("unsupported reference " + reference);
            }
        }

        return foundData;
    }
}
