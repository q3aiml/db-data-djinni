package net.q3aiml.dbdata.model;

public abstract class TableToTableReference {
    /**
     * The table being referenced.
     */
    public abstract Table getReferencedTable();

    /**
     * The table holding the reference.
     */
    public abstract Table getReferencingTable();
}
