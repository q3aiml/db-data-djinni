package net.q3aiml.dbdata.export;

import net.q3aiml.dbdata.config.DatabaseConfig;
import net.q3aiml.dbdata.introspect.DefaultDatabaseIntrospector;
import net.q3aiml.dbdata.jdbc.TableDataQueryer;
import net.q3aiml.dbdata.jdbc.UnpreparedStatement;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.model.ForeignKeyReference;
import net.q3aiml.dbdata.model.Table;
import net.q3aiml.dbdata.model.TableData;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class ExporterTest {
    DataSource dataSource = mock(DataSource.class);
    Connection c = mock(Connection.class);
    DatabaseConfig config = new DatabaseConfig();
    DefaultDatabaseIntrospector introspector = mock(DefaultDatabaseIntrospector.class);
    ReferencedDataLookup lookup = mock(ReferencedDataLookup.class);
    TableDataQueryer queryer = mock(TableDataQueryer.class);
    Exporter exporter = new Exporter(dataSource, config, introspector, lookup, queryer);
    DatabaseMetadata metadata = new DatabaseMetadata();

    Table user = new Table("schema", "user");
    Table group = new Table("schema", "group");
    Table groupUser = new Table("schema", "group_user");

    ForeignKeyReference groupRef = new ForeignKeyReference(groupUser, asList("group_fk"), group, asList("group_pk"));
    ForeignKeyReference userRef = new ForeignKeyReference(groupUser, asList("user_fk"), user, asList("user_pk"));

    @Before
    public void setUp() throws Exception {
        metadata.addTable(user);
        metadata.addTable(group);
        metadata.addTable(groupUser);
        metadata.addReference(groupRef);
        metadata.addReference(userRef);
    }

    @Test
    public void extractFollowReferencesTest() throws SQLException {
        TableData groupUserData = mock(TableData.class);
        when(groupUserData.table()).thenReturn(groupUser);
        when(groupUserData.rowCount()).thenReturn(2);

        ReferencedDataLookupInfo groupRefLookup = new ReferencedDataLookupInfo(groupRef, groupUser);
        UnpreparedStatement ps1 = mock(UnpreparedStatement.class);
        UnpreparedStatement ps2 = mock(UnpreparedStatement.class);
        when(lookup.query(groupRefLookup, groupUserData, 0)).thenReturn(ps1);
        when(lookup.query(groupRefLookup, groupUserData, 1)).thenReturn(ps2);
        TableData group1 = mock(TableData.class, "group1");
        TableData group2 = mock(TableData.class, "group2");
        when(queryer.execute(c, ps1, group)).thenReturn(group1);
        when(queryer.execute(c, ps2, group)).thenReturn(group2);

        ReferencedDataLookupInfo userRefLookup = new ReferencedDataLookupInfo(userRef, groupUser);
        UnpreparedStatement ps3 = mock(UnpreparedStatement.class);
        when(lookup.query(userRefLookup, groupUserData, 0)).thenReturn(ps3);
        when(lookup.query(userRefLookup, groupUserData, 1)).thenReturn(ps3);
        TableData user1 = mock(TableData.class, "user1");
        when(queryer.execute(c, ps3, user)).thenReturn(user1);
        when(queryer.execute(c, ps3, user)).thenReturn(user1);

        List<TableData> tableDatas = exporter.extractFollowReferences(c, metadata, groupUserData, new HashSet<>());

        assertEquals(asList(group1, group2, user1), tableDatas);
    }
}