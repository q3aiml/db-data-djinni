package net.q3aiml.dbdata.cli;

import com.github.rvesse.airline.Arguments;
import com.github.rvesse.airline.Command;
import com.github.rvesse.airline.Option;
import com.google.common.io.Files;
import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.config.DatabaseConfig;
import net.q3aiml.dbdata.introspect.DefaultDatabaseIntrospector;
import net.q3aiml.dbdata.jdbc.UnpreparedStatement;
import net.q3aiml.dbdata.jdbc.UnpreparedStatementExecutor;
import net.q3aiml.dbdata.modify.DataSpecModifySql;
import net.q3aiml.dbdata.modify.Importer;
import net.q3aiml.dbdata.verify.Verifier;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "import", description = "Import a set of data from a file to a database")
public class ImportCli implements Callable<Void> {
    @Inject
    SqlConnectionOption sqlConnectionOption;
    @Inject
    DatabaseConfigOption databaseConfigOption;

    @Option(name = "--for-reals")
    public boolean forReals;

    @Option(name = { "-v", "-verbose" })
    public boolean verbose = true;

    @Arguments
    public File dataFile;

    @Override
    public Void call() throws IOException, SQLException {
        DatabaseConfig databaseConfig = databaseConfigOption.databaseConfig();
        DefaultDatabaseIntrospector introspector = new DefaultDatabaseIntrospector(databaseConfig.schema);
        Importer importer = new Importer(introspector, new Verifier(introspector), new DataSpecModifySql());

        String yaml = Files.toString(this.dataFile, Charset.defaultCharset());
        DataSpec dataSpec = DataSpec.fromYaml(yaml);

        try (Connection c = sqlConnectionOption.dataSource().getConnection()) {
            List<UnpreparedStatement> changes = importer.generateChangesToApplyDataSpecToDatabase(
                    c, dataSpec, databaseConfig);

            for (UnpreparedStatement change : changes) {
                if (verbose) {
                    System.out.println(change.unsafeUnparameterizedSql());
                }
                if (forReals) {
                    new UnpreparedStatementExecutor().execute(c, change);
                }
            }
        }

        return null;
    }
}
