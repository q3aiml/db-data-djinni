package net.q3aiml.dbdata.jdbc;

import net.q3aiml.dbdata.model.Table;
import net.q3aiml.dbdata.model.TableData;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TableDataQueryer {
    public TableData execute(Connection c, UnpreparedStatement query, Table table) throws SQLException {
        PreparedStatement ps = c.prepareStatement(query.sql());
        int i = 1;
        for (Object value : query.values()) {
            ps.setObject(i++, value);
        }
        try (ResultSet rs = ps.executeQuery()) {
            return TableData.copyOfResultSet(table, rs);
        }
    }
}
