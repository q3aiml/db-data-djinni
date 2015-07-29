package net.q3aiml.dbdata.cli;

import com.github.rvesse.airline.Option;
import com.github.rvesse.airline.OptionType;
import com.google.common.io.Files;
import net.q3aiml.dbdata.config.SqlConnectionInfo;
import net.q3aiml.dbdata.jdbc.DataSources;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class SqlConnectionOption {
    @Option(name = "-c", type = OptionType.GLOBAL, title = "sql-connection-file",
            description = "file containing database connection info", required = true)
    public File sqlConnectionFile;

    public DataSource dataSource() throws IOException {
        SqlConnectionInfo connectionInfo;
        try {
            String content = Files.toString(sqlConnectionFile, Charset.defaultCharset());
            connectionInfo = SqlConnectionInfo.fromYaml(content);
        } catch (IOException e) {
            throw new InvalidArgumentException("error reading sql connection file "
                    + sqlConnectionFile.getAbsolutePath() + ": " + e.getMessage(), e);
        }
        return DataSources.toDataSource(connectionInfo);
    }
}
