package net.q3aiml.dbdata.jdbc;

import org.junit.Test;

import java.util.ArrayList;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class UnpreparedStatementTest {
    @Test
    public void nullValueTest() {
        ArrayList<Object> values = new ArrayList<>();
        values.add(null);
        new UnpreparedStatement("test sql", values);
    }

    @Test
    public void unsafeUnparameterizedSqlTest() {
        UnpreparedStatement unpreparedStatement = new UnpreparedStatement("test ? wee ?", asList("1", "2"));
        assertEquals("test '1' wee '2'", unpreparedStatement.unsafeUnparameterizedSql());
    }
}