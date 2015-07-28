package net.q3aiml.dbdata.model;

import com.google.common.collect.ImmutableList;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;

public class TableData {
    private Table table;
    public String[] columnNames;
    public String[][] data;

    public TableData() { }

    public TableData(Table table, String[] columnNames, String[][] data) {
        this.table = table;
        this.columnNames = columnNames;
        this.data = data;
    }

    public Table table() {
        return table;
    }

    public TableData setTable(Table table) {
        this.table = table;
        return this;
    }

    public int rowCount() {
        return data.length;
    }

    public String data(int row, int column) {
        return data[row][column];
    }

    public String data(int row, String columnName) {
        int column = Arrays.asList(columnNames).indexOf(columnName);
        if (column < 0) {
            throw new IllegalArgumentException("don't have data for column " + columnName);
        }
        return data(row, column);
    }

    public static TableData copyOfResultSet(Table table, ResultSet rs) throws SQLException {
        TableData tableData = new TableData();
        tableData.table = table;
        int columns = rs.getMetaData().getColumnCount();
        tableData.columnNames = new String[columns];
        for (int i = 0; i < columns; i++) {
            tableData.columnNames[i] = rs.getMetaData().getColumnName(i + 1);
        }

        ImmutableList.Builder<String[]> result = ImmutableList.builder();
        while (rs.next()) {
            String[] row = new String[columns];
            for (int i = 0; i < columns; i++) {
                row[i] = rs.getString(i + 1);
            }
            result.add(row);
        }
        tableData.data = result.build().toArray(new String[0][0]);
        return tableData;
    }

    @Override
    public String toString() {
        return "TableData{" +
                "table='" + table + '\'' +
                ", columnNames=" + Arrays.toString(columnNames) +
                ", data=" + data.length + " rows" +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableData)) return false;
        TableData tableData = (TableData)o;
        return Objects.equals(table, tableData.table) &&
                Objects.equals(columnNames, tableData.columnNames) &&
                Objects.equals(data, tableData.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, columnNames, data);
    }
}
