package net.q3aiml.dbdata.morph;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.model.Table;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SortDataSpecTest {
    DatabaseMetadata db = mock(DatabaseMetadata.class);
    Table beforeTable = new Table(null, "before");
    Table afterTable = new Table(null, "after");

    @Before
    public void setUp() throws Exception {
        when(db.tableByNameNoCreate("before")).thenReturn(beforeTable);
        when(db.tableByNameNoCreate("after")).thenReturn(afterTable);
    }

    @Test
    public void sortRowTablesTest() {
        when(db.tablesOrderedByReferences()).thenReturn(ImmutableList.of(beforeTable, afterTable));
        DataSpec dataSpec = new DataSpec();
        DataSpec.DataSpecRow afterRow = new DataSpec.DataSpecRow("after", ImmutableMap.of());
        DataSpec.DataSpecRow beforeRow = new DataSpec.DataSpecRow("before", ImmutableMap.of());
        dataSpec.tableRows.add(afterRow);
        dataSpec.tableRows.add(beforeRow);
        new SortDataSpec().sortRowTables(dataSpec, db);
        assertEquals(asList(beforeRow, afterRow), dataSpec.tableRows);
    }

    @Test
    public void sortRowTablesEmptyTest() {
        when(db.tablesOrderedByReferences()).thenReturn(ImmutableList.of());
        DataSpec dataSpec = new DataSpec();
        new SortDataSpec().sortRowTables(dataSpec, db);
    }
}
