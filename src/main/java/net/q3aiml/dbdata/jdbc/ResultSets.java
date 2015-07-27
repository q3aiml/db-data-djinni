package net.q3aiml.dbdata.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ResultSets {
    public static Function<ResultSet, Map<String, String>> toMap(ResultSetMetaData metaData) throws SQLException {
        int columnCount = metaData.getColumnCount();
        List<String> columnNames = IntStream.rangeClosed(1, columnCount)
                .mapToObj(i -> {
                    try {
                        return metaData.getColumnName(i);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());

        // Collectors.toMap doesn't like null values. need to do it this way
        return resultSet -> {
            try {
                Map<String, String> row = new HashMap<>();
                for (String columnName : columnNames) {
                    row.put(columnName, resultSet.getString(columnName));
                }
                return row;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
