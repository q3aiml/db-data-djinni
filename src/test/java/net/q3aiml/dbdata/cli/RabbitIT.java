package net.q3aiml.dbdata.cli;

import com.google.common.io.Resources;
import net.q3aiml.dbdata.HsqlTestDatabase;
import net.q3aiml.dbdata.config.DatabaseConfig;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RabbitIT {
    @Rule
    public HsqlTestDatabase db = new HsqlTestDatabase();

    SqlConnectionOption sqlConnectionOption = mock(SqlConnectionOption.class);
    DatabaseConfigOption databaseConfigOption = mock(DatabaseConfigOption.class);
    DatabaseConfig databaseConfig = new DatabaseConfig();
    DatabaseMetadata databaseMetadata = new DatabaseMetadata();
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    @Before
    public void setUp() throws Exception {
        db.execute("rabbit_it/schema.sql");
        db.execute("rabbit_it/data.sql");

        when(sqlConnectionOption.dataSource()).thenReturn(db.dataSource());
        when(databaseConfigOption.databaseConfig()).thenAnswer(args -> databaseConfig);
    }

    public String output() throws UnsupportedEncodingException {
        return out.toString(StandardCharsets.UTF_8.name());
    }

    public void loadConfig(String name) throws IOException {
        databaseConfig = DatabaseConfig.fromYaml(Resources.toString(Resources.getResource(name),
                StandardCharsets.UTF_8));
    }

    @Test
    public void exportNoConfigTest() throws Exception {
        ExportCli exportCli = new ExportCli();
        exportCli.sqlConnectionOption = sqlConnectionOption;
        exportCli.databaseConfigOption = databaseConfigOption;
        exportCli.out = new PrintStream(out, true, StandardCharsets.UTF_8.name());
        exportCli.startTableQuery = asList("public.rabbit", "where name = 'Truffles'");
        exportCli.call();

        assertEquals(Resources.toString(Resources.getResource("rabbit_it/export_default.yaml"), StandardCharsets.UTF_8).trim(),
                output().trim());
    }

    @Test
    public void exportWithConfigTest() throws Exception {
        loadConfig("rabbit_it/config.yaml");
        ExportCli exportCli = new ExportCli();
        exportCli.sqlConnectionOption = sqlConnectionOption;
        exportCli.databaseConfigOption = databaseConfigOption;
        exportCli.out = new PrintStream(out, true, StandardCharsets.UTF_8.name());
        exportCli.startTableQuery = asList("public.rabbit", "where name = 'Truffles'");
        exportCli.call();

        assertEquals(Resources.toString(Resources.getResource("rabbit_it/export_with_config.yaml"), StandardCharsets.UTF_8).trim(),
                output().trim());
    }

    @Test
    public void verifyWithConfigTest() throws IOException, SQLException, URISyntaxException {
        loadConfig("rabbit_it/config.yaml");
        VerifyCli verifyCli = new VerifyCli();
        verifyCli.sqlConnectionOption = sqlConnectionOption;
        verifyCli.databaseConfigOption = databaseConfigOption;
        // TODO fix to not assume resource is available as file
        verifyCli.file = new File(Resources.getResource("rabbit_it/export_with_config.yaml").toURI());
        verifyCli.call();
    }
}
