package net.q3aiml.dbdata.export;

import net.q3aiml.dbdata.jdbc.UnpreparedStatement;
import net.q3aiml.dbdata.model.Table;
import net.q3aiml.dbdata.model.TableData;
import org.junit.Test;

import java.sql.SQLException;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class ReferencedDataLookupTest {
    ReferencedDataLookup lookup = new ReferencedDataLookup();

    Table groupUser = new Table("schema", "groupUser");
    Table group = new Table("schema", "group");
    TableData groupUserData = new TableData(groupUser, new String[] { "group_fk", "user_fk" },
            new String[][] {
                    { "group1", "user1" },
                    { "group1", "user2" },
                    { "group2", "user1" },
            });

    @Test
    public void test() throws SQLException {
        ReferencedDataLookupInfo groupLookupInfo = new ReferencedDataLookupInfo(group,
                asList("group_pk"), asList("group_fk"));
        ReferencedDataLookupInfo userLookupInfo = new ReferencedDataLookupInfo(group,
                asList("user_pk"), asList("user_fk"));

        UnpreparedStatement query1 = lookup.query(groupLookupInfo, groupUserData, 0);
        assertEquals("SELECT * FROM schema.group WHERE group_pk = ?", query1.sql());
        assertEquals(asList("group1"), query1.values());

        UnpreparedStatement query2 = lookup.query(groupLookupInfo, groupUserData, 1);
        assertEquals("SELECT * FROM schema.group WHERE group_pk = ?", query2.sql());
        assertEquals(asList("group1"), query2.values());

        UnpreparedStatement query3 = lookup.query(groupLookupInfo, groupUserData, 2);
        assertEquals("SELECT * FROM schema.group WHERE group_pk = ?", query3.sql());
        assertEquals(asList("group2"), query3.values());

        UnpreparedStatement query4 = lookup.query(userLookupInfo, groupUserData, 1);
        assertEquals("SELECT * FROM schema.group WHERE user_pk = ?", query4.sql());
        assertEquals(asList("user2"), query4.values());
    }
}