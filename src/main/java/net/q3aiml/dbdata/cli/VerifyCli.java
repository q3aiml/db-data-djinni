package net.q3aiml.dbdata.cli;

import com.github.rvesse.airline.Arguments;
import com.github.rvesse.airline.Command;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.config.DatabaseConfig;
import net.q3aiml.dbdata.introspect.DefaultDatabaseIntrospector;
import net.q3aiml.dbdata.verify.VerificationError;
import net.q3aiml.dbdata.verify.Verifier;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "verify", description = "Verify database matches data in a file")
public class VerifyCli implements Callable<Void> {
    SqlConnectionOption sqlConnectionOption = new SqlConnectionOption();

    @Arguments
    public File file;

    @Override
    public Void call() throws IOException, SQLException {
        String yaml;
        try {
            yaml = Files.toString(file, Charset.defaultCharset());
        } catch (IOException e) {
            throw new IOException("error reading data file " + file.getAbsolutePath() + ": " + e.getMessage(), e);
        }
        DataSpec dataSpec = DataSpec.fromYaml(yaml);

        DataSource dataSource = sqlConnectionOption.dataSource();

        DatabaseConfig serializeConfig = DatabaseConfig
                .fromYaml(Resources.toString(Resources.getResource("config.yaml"), StandardCharsets.UTF_8));

        DefaultDatabaseIntrospector introspector = new DefaultDatabaseIntrospector(serializeConfig.schema);
        Verifier verifier = new Verifier(introspector);
        List<VerificationError> errors = verifier.verify(dataSource, dataSpec, serializeConfig);

        if (errors.isEmpty()) {
            System.out.println("verification passed");
        } else {
            System.out.println("verification failed " + errors);
            System.exit(1);
        }

        return null;
    }
}
