package net.q3aiml.dbdata.export;

import net.q3aiml.dbdata.model.ForeignKeyReference;
import net.q3aiml.dbdata.model.Table;
import org.junit.Test;

import java.sql.SQLException;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class ReferencedDataLookupInfoTest {
    Table group = new Table("schema", "group");
    Table groupUser = new Table("schema", "group_user");
    ForeignKeyReference groupRef = new ForeignKeyReference(groupUser, asList("group_fk"), group, asList("group_pk"));

    @Test
    public void test() throws SQLException {
        ReferencedDataLookupInfo lookup = new ReferencedDataLookupInfo(groupRef, group);
        assertEquals(groupUser, lookup.otherTable);
        assertEquals(asList("group_fk"), lookup.otherColumns);
        assertEquals(asList("group_pk"), lookup.ourColumns);

        ReferencedDataLookupInfo lookup2 = new ReferencedDataLookupInfo(groupRef, groupUser);
        assertEquals(group, lookup2.otherTable);
        assertEquals(asList("group_pk"), lookup2.otherColumns);
        assertEquals(asList("group_fk"), lookup2.ourColumns);
    }
}