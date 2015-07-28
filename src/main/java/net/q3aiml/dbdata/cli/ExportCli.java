package net.q3aiml.dbdata.cli;

import com.github.rvesse.airline.Arguments;
import com.github.rvesse.airline.Command;
import net.q3aiml.dbdata.config.DatabaseConfig;
import net.q3aiml.dbdata.export.Exporter;

import javax.inject.Inject;
import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "export", description = "Export a set of data from a database to a file")
public class ExportCli implements Callable<Void> {
    @Inject
    SqlConnectionOption sqlConnectionOption;
    @Inject
    DatabaseConfigOption databaseConfigOption;

    @Arguments(title = { "start-table", "start-query" }, required = true)
    public List<String> startTableQuery;

    @Override
    public Void call() throws IOException, SQLException {
        DataSource dataSource = sqlConnectionOption.dataSource();
        DatabaseConfig config = databaseConfigOption.databaseConfig();

        Exporter exporter = new Exporter(dataSource, config);
        String yaml = exporter.extractAndSerialize(startTableQuery.get(0), startTableQuery.get(1));
        System.out.println(yaml);

        return null;
    }
}
