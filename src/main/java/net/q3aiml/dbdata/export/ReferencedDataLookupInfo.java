package net.q3aiml.dbdata.export;

import net.q3aiml.dbdata.model.ForeignKeyReference;
import net.q3aiml.dbdata.model.Table;

import java.util.List;
import java.util.Objects;

/*package*/ class ReferencedDataLookupInfo {
    public final Table otherTable;
    public final List<String> otherColumns;
    public final List<String> ourColumns;

    public ReferencedDataLookupInfo(ForeignKeyReference fk, Table referencingTable) {
        if (fk.getReferencedTable() == referencingTable) {
            otherTable = fk.getReferencingTable();
            otherColumns = fk.getReferencingColumns();
            ourColumns = fk.getReferencedColumns();
        } else {
            otherTable = fk.getReferencedTable();
            otherColumns = fk.getReferencedColumns();
            ourColumns = fk.getReferencingColumns();
        }
    }

    public ReferencedDataLookupInfo(Table otherTable, List<String> otherColumns, List<String> ourColumns) {
        this.otherTable = otherTable;
        this.otherColumns = otherColumns;
        this.ourColumns = ourColumns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReferencedDataLookupInfo)) return false;
        ReferencedDataLookupInfo that = (ReferencedDataLookupInfo)o;
        return Objects.equals(otherTable, that.otherTable) &&
                Objects.equals(otherColumns, that.otherColumns) &&
                Objects.equals(ourColumns, that.ourColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(otherTable, otherColumns, ourColumns);
    }
}
