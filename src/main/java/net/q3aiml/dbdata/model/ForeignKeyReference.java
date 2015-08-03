package net.q3aiml.dbdata.model;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public class ForeignKeyReference extends TableToTableReference {
    private final Table primaryKeyTable;
    private final List<String> primaryKeyColumns;
    private final Table foreignKeyTable;
    private final List<String> foreignKeyColumns;

    public ForeignKeyReference(Table pkTable, List<String> primaryKeyColumns, Table toTable, List<String> foreignKeyColumns) {
        checkArgument(primaryKeyColumns.size() == foreignKeyColumns.size(),
                "primary and foreign key column lists must be same size");

        this.primaryKeyTable = pkTable;
        this.primaryKeyColumns = primaryKeyColumns;
        this.foreignKeyTable = toTable;
        this.foreignKeyColumns = foreignKeyColumns;
    }

    @JsonCreator
    public ForeignKeyReference(
            @JacksonInject("metadata")
            DatabaseMetadata db,
            @JsonProperty("pk_table")
            String pkTable,
            @JsonProperty("pk_columns")
            List<String> primaryKeyColumns,
            @JsonProperty("fk_table")
            String toTable,
            @JsonProperty("fk_columns")
            List<String> foreignKeyColumns)
    {
        this(db.tableByName(pkTable), primaryKeyColumns, db.tableByName(toTable), foreignKeyColumns);
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
