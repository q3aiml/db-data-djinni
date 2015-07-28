package net.q3aiml.dbdata.jdbc;

import org.junit.Test;

import java.util.ArrayList;

public class UnpreparedStatementTest {
    @Test
    public void nullValueTest() {
        ArrayList<Object> values = new ArrayList<>();
        values.add(null);
        new UnpreparedStatement("test sql", values);
    }
}