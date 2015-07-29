package net.q3aiml.dbdata.export;

import net.q3aiml.dbdata.jdbc.UnpreparedStatement;
import net.q3aiml.dbdata.model.TableData;

import java.util.List;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Look up a referenced row
 */
/*package*/ class ReferencedDataLookup {
    /**
     * sql to lookup data on the other side of {@code referencingTable}
     */
    protected static String sql(ReferencedDataLookupInfo info) {
        return "SELECT * FROM " + info.otherTable.schema + "." + info.otherTable.name + " WHERE "
                + info.otherColumns.stream().map(col -> col + " = ?").collect(joining(" AND "));
    }

    public UnpreparedStatement query(ReferencedDataLookupInfo lookupInfo, TableData tableData, int rowIndex) {
        String sql = sql(lookupInfo);
        List<String> values = lookupInfo.ourColumns.stream()
                .map(ourColumn -> tableData.data(rowIndex, ourColumn))
                .collect(toList());
        return new UnpreparedStatement(sql, values);
    }
}
