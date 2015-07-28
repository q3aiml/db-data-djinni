package net.q3aiml.dbdata.jdbc;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public class UnpreparedStatement {
    private final String sql;
    private final ImmutableList<?> values;

    public UnpreparedStatement(String sql, List<?> values) {
        this.sql = sql;
        this.values = ImmutableList.copyOf(values);
    }

    public String sql() {
        return sql;
    }

    public ImmutableList<?> values() {
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
