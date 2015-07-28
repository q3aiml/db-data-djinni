package net.q3aiml.dbdata.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class UnpreparedStatementExecutor {
    public int execute(Connection c, UnpreparedStatement query) throws SQLException {
        PreparedStatement ps = c.prepareStatement(query.sql());
        int i = 1;
        for (Object value : query.values()) {
            ps.setObject(i++, value);
        }
        return ps.executeUpdate();
    }
}
