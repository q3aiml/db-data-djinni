package net.q3aiml.dbdata.cli;

import com.github.rvesse.airline.Option;
import com.github.rvesse.airline.OptionType;
import com.google.common.io.Files;
import net.q3aiml.dbdata.config.DatabaseConfig;
import net.q3aiml.dbdata.model.DatabaseMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class DatabaseConfigOption {
    public DatabaseMetadata databaseMetadata = new DatabaseMetadata();

    @Option(name = "-t", type = OptionType.GLOBAL, title = "database-config-file",
            description = "database config file", required = true)
    public File databaseConfigFile;

    public DatabaseConfig databaseConfig() throws IOException {
        String content;
        try {
            content = Files.toString(databaseConfigFile, Charset.defaultCharset());
        } catch (IOException e) {
            throw new InvalidArgumentException("error reading database config file "
                    + databaseConfigFile.getAbsolutePath() + ": " + e.getMessage(), e);
        }
        // jackson errors aren't the best, especially alone. we probably want whatever is chained
        return DatabaseConfig.fromYaml(content, databaseMetadata);
    }
}
