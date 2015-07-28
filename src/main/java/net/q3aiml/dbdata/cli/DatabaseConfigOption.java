package net.q3aiml.dbdata.cli;

import com.github.rvesse.airline.Option;
import com.google.common.io.Files;
import net.q3aiml.dbdata.config.DatabaseConfig;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class DatabaseConfigOption {
    @Option(name = "-t", title = "database-config-file", description = "database config file", required = true)
    public File databaseConfigFile;

    public DatabaseConfig databaseConfig() throws IOException {
        try {
            String content = Files.toString(databaseConfigFile, Charset.defaultCharset());
            return DatabaseConfig.fromYaml(content);
        } catch (IOException e) {
            throw new IOException("error reading database config file " + databaseConfigFile.getAbsolutePath()
                    + ": " + e.getMessage(), e);
        }
    }
}
