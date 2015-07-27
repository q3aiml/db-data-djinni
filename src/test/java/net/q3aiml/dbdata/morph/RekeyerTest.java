package net.q3aiml.dbdata.morph;

import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.JdbcMock;
import net.q3aiml.dbdata.config.RekeyerConfig;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class RekeyerTest {
    Connection c = mock(Connection.class);
    RekeyerConfig config = new RekeyerConfig();
    DatabaseMetadata db = new DatabaseMetadata();
    PreparedStatement ps = mock(PreparedStatement.class);

    @Test
    public void rekeyerTest() throws SQLException {
        DataSpec dataSpec = new DataSpec();
        DataSpec.DataSpecRow row = new DataSpec.DataSpecRow();
        row.setTable("testtable");
        row.getRow().put("id", "123");
        row.getRow().put("altkey", "Green");
        dataSpec.tableRows.add(row);

        db.table(null, "testtable").setPrimaryKeyColumns(asList("id"));
        config.altKeys.put("testtable", asList("altkey"));

        when(c.prepareStatement("SELECT * FROM testtable WHERE altkey = ?")).thenReturn(ps);
        ResultSet rs = JdbcMock.resultSet(
                asList("id", "altkey"),
                asList(
                        asList("321", "Green")
                )
        );
        when(ps.executeQuery()).thenReturn(rs);

        new Rekeyer().rekey(dataSpec, c, config, db);

        verify(ps).setObject(1, "Green");
        assertEquals("321", row.getRow().get("id"));
    }
}