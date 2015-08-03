package net.q3aiml.dbdata.morph;

import com.google.common.collect.ImmutableMap;
import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.JdbcMock;
import net.q3aiml.dbdata.config.RekeyerConfig;
import net.q3aiml.dbdata.jdbc.UnpreparedStatement;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.model.ForeignKeyReference;
import net.q3aiml.dbdata.model.Table;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class RekeyerTest {
    Connection c = mock(Connection.class, RETURNS_SMART_NULLS);
    RekeyerConfig config = new RekeyerConfig();
    DatabaseMetadata db = new DatabaseMetadata();
    PreparedStatement ps = mock(PreparedStatement.class);

    Rekeyer rekeyer = new Rekeyer();

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

        when(c.prepareStatement("SELECT testtable.* FROM testtable WHERE testtable.altkey = ?")).thenReturn(ps);
        ResultSet rs = JdbcMock.resultSet(
                asList("id", "altkey"),
                asList(
                        asList("321", "Green")
                )
        );
        when(ps.executeQuery()).thenReturn(rs);

        rekeyer.rekey(dataSpec, c, config, db);

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

        when(c.prepareStatement("SELECT rabbit.* FROM rabbit WHERE rabbit.name = ? AND rabbit.breed_fk = ?"))
                .thenReturn(ps);
        ResultSet rs = JdbcMock.resultSet(
                asList("rabbit_pk", "breed_fk"),
                asList(
                        asList("1000", "2")
                )
        );
        when(ps.executeQuery()).thenReturn(rs);

        rekeyer.rekey(dataSpec, c, config, db);

        verify(ps).setObject(1, "Truffles");
        verify(ps).setObject(2, "2");
        assertEquals("1000", rabbitRow.getRow().get("rabbit_pk"));
    }

    @Test
    public void parseAltKeyColumnTest() {
        Table rabbit = db.table("schema", "rabbit");
        Table owner = db.table("schema", "owner");
        ForeignKeyReference reference = new ForeignKeyReference(owner, asList("owner_pk"), rabbit, asList("owner_fk"));
        db.addReference(reference);

        Rekeyer.AltKeyColumn altKeyColumn = rekeyer.parseAltKeyColumn(rabbit, "owner_fk.name", db);
        assertEquals(rabbit, altKeyColumn.startTable);
        assertEquals(asList("owner_fk", "name"), altKeyColumn.altKeyColumnPath);
        assertEquals(asList(reference), altKeyColumn.referencePath);
    }

    @Test
    public void altKeyQuerySimpleValueTest() {
        Table rabbit = new Table("schema", "rabbit");
        Map<Rekeyer.AltKeyColumn, Optional<Object>> columnValues = new LinkedHashMap<>();
        columnValues.put(new Rekeyer.AltKeyColumn(rabbit, asList("name"), asList()),
                Optional.of("truffles"));
        UnpreparedStatement query = rekeyer.altKeyQuery("rabbit", columnValues, db);
        assertEquals("SELECT rabbit.* FROM rabbit WHERE schema.rabbit.name = ?", query.sql());
        assertEquals(asList("truffles"), query.values());
    }

    @Test
    public void altKeyQueryJoinTest() {
        Table rabbit = new Table("schema", "rabbit");
        Table owner = new Table("schema", "owner");
        Map<Rekeyer.AltKeyColumn, Optional<Object>> columnValues = new LinkedHashMap<>();
        columnValues.put(new Rekeyer.AltKeyColumn(rabbit, asList("owner_fk", "name"),
                        asList(new ForeignKeyReference(owner, asList("owner_pk"), rabbit, asList("owner_fk")))),
                Optional.of("name_value"));
        UnpreparedStatement query = rekeyer.altKeyQuery("rabbit", columnValues, db);
        assertEquals("SELECT rabbit.* FROM rabbit "
                + "LEFT JOIN schema.owner ON (schema.owner.owner_pk = schema.rabbit.owner_fk) "
                + "WHERE schema.owner.name = ?", query.sql());
        assertEquals(asList("name_value"), query.values());
    }
}