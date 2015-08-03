package net.q3aiml.dbdata.morph;

import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.model.Table;

import java.util.HashSet;

public class PrimaryKeyFilter {
    public void filterPrimaryKeys(DataSpec dataSpec, DatabaseMetadata db) {
        for (DataSpec.DataSpecRow row : dataSpec.tableRows) {
            Table table = db.tableByNameNoCreate(row.getTable());
            for (String key : new HashSet<>(row.getRow().keySet())) {
                if (table.primaryKeyColumns().contains(key)
                        // it's both primary and foreign. leave it in
                        && !(row.getRow().get(key) instanceof DataSpec.DataSpecRow))
                {
                    row.getRow().remove(key);
                }
            }
        }
    }
}
