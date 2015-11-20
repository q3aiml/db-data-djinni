package net.q3aiml.dbdata.jdbc;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sql statement and values of placeholders in the sql.
 */
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

    /**
     * sql statement with parameters inlined for pretty printing (unsafe, best effort, and almost certainly
     * shouldn't be used for actual execution)
     */
    public String unsafeUnparameterizedSql() {
        Matcher m = Pattern.compile("\\?").matcher(sql);
        Iterator<?> iterValues = values.iterator();
        StringBuffer buf = new StringBuffer();
        while (m.find()) {
            Object value = iterValues.next();
            String stringValue;
            if (value == null) {
                stringValue = "NULL";
            } else {
                stringValue = "'" + value.toString().replace("'", "''") + "'";
            }
            m.appendReplacement(buf, Matcher.quoteReplacement(stringValue));
        }
        m.appendTail(buf);
        return buf.toString();
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
