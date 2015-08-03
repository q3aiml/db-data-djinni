package net.q3aiml.dbdata;

import com.google.common.io.Resources;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkState;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HsqlTestDatabase implements MethodRule {
    private Connection connection;

    public Connection connection() {
        checkState(connection != null, "no connection: " + HsqlTestDatabase.class + " must be annotated with @Rule");
        return connection;
    }

    public DataSource dataSource() throws SQLException {
        JDBCDataSource dataSource = new JDBCDataSource();
        dataSource.setUrl(jdbcUrl());
        return dataSource;
    }

    protected String jdbcUrl() {
        return "jdbc:hsqldb:mem:test;shutdown=true";
    }

    private AutoCloseable connect() throws SQLException {
        connection = DriverManager.getConnection(jdbcUrl());
        try (java.sql.Statement st = connection.createStatement()) {
            st.execute("drop schema public cascade");
        }

        return () -> {
            try {
                try (java.sql.Statement st = connection.createStatement()) {
                    st.execute("shutdown");
                }
                connection.close();
            } finally {
                connection = null;
            }
        };
    }

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object target) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                //noinspection unused
                try (AutoCloseable closeConnection = connect()) {
                    base.evaluate();
                }
            }
        };
    }

    public void execute(String resource) throws SQLException, IOException {
        String sql = Resources.toString(Resources.getResource(resource), StandardCharsets.UTF_8);
        try (java.sql.Statement st = connection.createStatement()) {
            st.execute(sql);
        }
    }
}
