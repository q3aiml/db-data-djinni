package net.q3aiml.dbdata.morph;

import com.google.common.collect.ImmutableMap;
import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.JdbcMock;
import net.q3aiml.dbdata.config.RekeyerConfig;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.model.ForeignKeyReference;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class RekeyerTest {
    Connection c = mock(Connection.class, RETURNS_SMART_NULLS);
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

    /**
     * support alt keys that contains a DataSpecRow (resolved by KeyResolver)
     */
    @Test
    public void rekeyerAltKeyWithDataSpecRowTest() throws SQLException {
        DataSpec dataSpec = new DataSpec();
        DataSpec.DataSpecRow rabbitRow = new DataSpec.DataSpecRow("rabbit", new HashMap<>(ImmutableMap.of(
                "rabbit_pk", "1",
                "name", "Truffles",
                "breed_fk", new DataSpec.DataSpecRow("breed", new HashMap<>(ImmutableMap.of(
                        "breed_pk", "2",
                        "name", "LionLop"
                )))
        )));
        dataSpec.tableRows.add(rabbitRow);

        db.table(null, "rabbit").setPrimaryKeyColumns(asList("rabbit_pk"));
        db.table(null, "breed").setPrimaryKeyColumns(asList("breed_pk"));
        config.altKeys.put("rabbit", asList("name", "breed_fk"));
        db.addReference(new ForeignKeyReference(
                db.table(null, "breed"), asList("breed_pk"), db.table(null, "rabbit"), asList("breed_fk")));

        when(c.prepareStatement("SELECT * FROM rabbit WHERE name = ? AND breed_fk = ?")).thenReturn(ps);
        ResultSet rs = JdbcMock.resultSet(
                asList("rabbit_pk"),
                asList(
                        asList("1000")
                )
        );
        when(ps.executeQuery()).thenReturn(rs);

        new Rekeyer().rekey(dataSpec, c, config, db);

        verify(ps).setObject(1, "Truffles");
        verify(ps).setObject(2, "2");
        assertEquals("1000", rabbitRow.getRow().get("rabbit_pk"));
    }
}