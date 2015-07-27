package net.q3aiml.dbdata.model;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Columns which uniquely identify a row in a table
 */
public class UniqueInfo {
    /**
     * Sets of columns that uniquely identify a row in a table
     */
    private Set<Set<String>> uniqueColumnSets = new LinkedHashSet<>();

    public Set<Set<String>> uniqueColumnSets() {
        return ImmutableSet.copyOf(uniqueColumnSets);
    }

    public void addUniqueColumnSet(ImmutableSet<String> uniqueColumnSet) {
        uniqueColumnSets.add(uniqueColumnSet);
    }

    public void add(UniqueInfo uniqueInfo) {
        uniqueColumnSets.addAll(uniqueInfo.uniqueColumnSets);
    }

    public Set<Set<String>> findAvailableUniqueColumnSets(Set<String> availableColumns) {
        return uniqueColumnSets().stream()
                .filter(uniqueColumnSet -> Sets.powerSet(availableColumns).contains(uniqueColumnSet))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public String toString() {
        return "UniqueInfo{" +
                "uniqueColumnSets=" + uniqueColumnSets +
                '}';
    }
}
