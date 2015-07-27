package net.q3aiml.dbdata;

import org.mockito.stubbing.Answer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class JdbcMock {
    /**
     * Example:
     * <pre><code>
     * resultSet(
     *     asList("col1", "col2"),
     *     asList(
     *         asList("row1_col1_value", "row1_col2_value"),
     *         ...
     *     )
     * )
     * </code></pre>
     *
     * @param columnNames names of columns matching the order in {@code rows}
     * @param rows each row as a list of values for the row. Must match position in {@code columnNames}
     */
    public static ResultSet resultSet(final List<String> columnNames, final List<? extends List<?>> rows) throws SQLException {
        ResultSet result = mock(ResultSet.class);

        AtomicInteger currentRow = new AtomicInteger(-1);
        when(result.next()).thenAnswer(aInvocation -> currentRow.incrementAndGet() < rows.size());

        Answer getValueAnswer = aInvocation -> {
            Object arg = aInvocation.getArguments()[0];
            int row = currentRow.get();
            int column;
            if (arg instanceof Integer) {
                column = (Integer)arg - 1;
            } else if (arg instanceof String) {
                //noinspection SuspiciousMethodCalls
                column = columnNames.indexOf(arg);
            } else {
                throw new IllegalArgumentException("unexpected arg " + arg.getClass());
            }
            return rows.get(row).get(column);
        };
        when(result.getString(anyString())).thenAnswer(getValueAnswer);
        when(result.getString(anyInt())).thenAnswer(getValueAnswer);

        return result;
    }
}