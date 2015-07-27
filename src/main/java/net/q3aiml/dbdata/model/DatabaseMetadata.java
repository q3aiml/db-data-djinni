package net.q3aiml.dbdata.model;

import com.google.common.collect.ImmutableList;
import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DirectedPseudograph;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class DatabaseMetadata {
    DirectedGraph<Table, TableToTableReference> graph = new DirectedPseudograph<>(TableToTableReference.class);
    Map<String, Table> tablesByName = new HashMap<>();

    public Iterable<Table> tables() {
        return tablesByName.values();
    }

    public Table table(String schema, String table) {
        String fullNormalizedName = qualifiedTableName(normalizeName(schema), normalizeName(table));
        if (!tablesByName.containsKey(fullNormalizedName)) {
            addTable(schema, table);
        }
        return tablesByName.get(fullNormalizedName);
    }

    public Table tableByNameNoCreate(String tableName) {
        tableName = normalizeName(tableName);
        Table table = tablesByName.get(tableName);
        if (table == null) {
            throw new NoSuchElementException("no table named '" + tableName + "'");
        }
        return table;
    }

    public Table addTable(String schemaName, String tableName) {
        Table table = new Table(schemaName, tableName);
        schemaName = normalizeName(schemaName);
        tableName = normalizeName(tableName);
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

    public void addReference(Table from, Table to, TableToTableReference reference) {
        if (reference == null) {
            graph.addEdge(from, to);
        } else {
            graph.addEdge(from, to, reference);
        }
    }

    public Set<TableToTableReference> referringTo(Table table) {
        return graph.incomingEdgesOf(table);
    }

    public Set<TableToTableReference> referredToBy(Table table) {
        return graph.outgoingEdgesOf(table);
    }

    public Set<TableToTableReference> references(Table table) {
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