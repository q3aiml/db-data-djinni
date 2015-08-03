package net.q3aiml.dbdata.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class UnpreparedStatementExecutor {
    public PreparedStatement prepare(Connection c, UnpreparedStatement query) throws SQLException {
        String sql = query.sql();
        PreparedStatement ps = c.prepareStatement(sql);
        int i = 1;
        for (Object value : query.values()) {
            try {
                ps.setObject(i++, value);
            } catch (SQLException e) {
                throw new SQLException("unable to set column " + i + " to " + value + ": " + e.getMessage()
                        + "\n\tsql: " + sql, e);
            }
        }
        return ps;
    }

    public int execute(Connection c, UnpreparedStatement query) throws SQLException {
        try (PreparedStatement ps = prepare(c, query)) {
            return ps.executeUpdate();
        }
    }
}
