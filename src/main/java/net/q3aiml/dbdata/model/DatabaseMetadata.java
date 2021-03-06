package net.q3aiml.dbdata.model;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DirectedPseudograph;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Tables, the relationship graph between tables, and other metadata.
 */
public class DatabaseMetadata {
    DirectedGraph<Table, TableToTableReference> graph = new DirectedPseudograph<>(TableToTableReference.class);
    Map<String, Table> tablesByName = new HashMap<>();

    public Iterable<Table> tables() {
        return tablesByName.values();
    }

    public Table table(String schema, String table) {
        String fullNormalizedName = qualifiedTableName(normalizeName(schema), normalizeName(table));
        if (!tablesByName.containsKey(fullNormalizedName)) {
            return addTable(new Table(schema, table));
        }
        return tablesByName.get(fullNormalizedName);
    }

    public Table tableByNameNoCreate(String tableName) {
        tableName = normalizeName(tableName);
        Table table = tablesByName.get(tableName);
        if (table == null) {
            throw new NoSuchElementException("no table named '" + tableName + "' " +
                    "(known tables: " + tablesByName.keySet() + ")");
        }
        return table;
    }

    public Table tableByName(String table) {
        List<String> strings = Splitter.on(".").limit(2).splitToList(table);
        if (strings.size() > 1) {
            return table(strings.get(0), strings.get(1));
        } else {
            return table(null, strings.get(0));
        }
    }

    public Table addTable(String schemaName, String tableName) {
        return table(schemaName, tableName);
    }

    public Table addTable(Table table) {
        String schemaName = normalizeName(table.schema);
        String tableName = normalizeName(table.name);
        tablesByName.put(qualifiedTableName(schemaName, tableName), table);
        graph.addVertex(table);
        return table;
    }

    private String normalizeName(String name) {
        return name != null ? name.toLowerCase() : null;
    }

    private String qualifiedTableName(String schema, String table) {
        return schema != null ? schema + "." + table : table;
    }

    public DirectedGraph<Table, TableToTableReference> graph() {
        return graph;
    }

    public void addReference(TableToTableReference reference) {
        checkNotNull(reference, "reference must not be null");
        graph.addEdge(reference.getReferencingTable(), reference.getReferencedTable(), reference);
    }

    public Set<TableToTableReference> referringTo(Table table) {
        checkNotNull(table, "table must not be null");
        return graph.incomingEdgesOf(table);
    }

    public Set<TableToTableReference> referredToBy(Table table) {
        checkNotNull(table, "table must not be null");
        return graph.outgoingEdgesOf(table);
    }

    public Set<TableToTableReference> references(Table table) {
        checkNotNull(table, "table must not be null");
        return graph.edgesOf(table);
    }

    public Set<TableToTableReference> references() {
        return graph.edgeSet();
    }

    public ImmutableList<Table> tablesOrderedByReferences() {
        Set<Table> cycles = new CycleDetector<>(graph).findCycles();
        if (!cycles.isEmpty()) {
            throw new IllegalStateException("unsupported: reference graph has cycles: " + cycles);
        }
        return ImmutableList.copyOf(new TopologicalOrderIterator<>(graph)).reverse();
    }

    @Override
    public String toString() {
        return "Database{" +
                "graph=" + graph +
                ", tablesByName=" + tablesByName +
                '}';
    }
}
