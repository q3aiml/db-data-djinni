package net.q3aiml.dbdata.jdbc;

import java.util.*;

public class UnpreparedStatement {
    private final String sql;
    private final List<?> values;

    public UnpreparedStatement(String sql, List<?> values) {
        this.sql = sql;
        // must be a null friendly collection
        this.values = Collections.unmodifiableList(new ArrayList<>(values));
    }

    public String sql() {
        return sql;
    }

    public List<?> values() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnpreparedStatement that = (UnpreparedStatement) o;
        return Objects.equals(sql, that.sql) &&
                Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql, values);
    }

    @Override
    public String toString() {
        return sql + " % " + values;
    }
}
