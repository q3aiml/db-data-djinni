package net.q3aiml.dbdata.morph;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.model.Table;

/**
 * Sorts rows in a {@link DataSpec} so as to avoid forward references.
 */
public class SortDataSpec {
    public void sortRowTables(DataSpec dataSpec, DatabaseMetadata db) {
        ImmutableList<Table> tableOrdering = db.tablesOrderedByReferences();
        dataSpec.tableRows.sort(Ordering.explicit(tableOrdering)
                .onResultOf(row -> db.tableByNameNoCreate(row.getTable())));
    }
}
