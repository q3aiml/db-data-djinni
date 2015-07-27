package net.q3aiml.dbdata.model;

import java.util.List;
import java.util.Objects;

public class ForeignKeyReference extends TableToTableReference {
    private final Table primaryKeyTable;
    private final List<String> primaryKeyColumns;
    private final Table foreignKeyTable;
    private final List<String> foreignKeyColumns;

    public ForeignKeyReference(Table pkTable, List<String> primaryKeyColumns, Table toTable, List<String> foreignKeyColumns) {
        this.primaryKeyTable = pkTable;
        this.primaryKeyColumns = primaryKeyColumns;
        this.foreignKeyTable = toTable;
        this.foreignKeyColumns = foreignKeyColumns;
    }

    @Override
    public Table getReferencedTable() {
        return primaryKeyTable;
    }

    public List<String> getReferencedColumns() {
        return primaryKeyColumns;
    }

    @Override
    public Table getReferencingTable() {
        return foreignKeyTable;
    }

    public List<String> getReferencingColumns() {
        return foreignKeyColumns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ForeignKeyReference)) return false;
        ForeignKeyReference that = (ForeignKeyReference)o;
        return Objects.equals(primaryKeyTable, that.primaryKeyTable) &&
                Objects.equals(primaryKeyColumns, that.primaryKeyColumns) &&
                Objects.equals(foreignKeyTable, that.foreignKeyTable) &&
                Objects.equals(foreignKeyColumns, that.foreignKeyColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryKeyTable, primaryKeyColumns, foreignKeyTable, foreignKeyColumns);
    }

    @Override
    public String toString() {
        return "ForeignKeyReference{" +
                "primaryKeyTable=" + primaryKeyTable.name +
                ", fromColumn='" + primaryKeyColumns + '\'' +
                ", foreignKeyTable=" + foreignKeyTable.name +
                ", toColumn='" + foreignKeyColumns + '\'' +
                "}";
    }
}
