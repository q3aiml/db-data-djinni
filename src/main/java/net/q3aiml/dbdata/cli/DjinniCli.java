package net.q3aiml.dbdata.cli;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.builder.CliBuilder;
import com.github.rvesse.airline.help.Help;
import com.github.rvesse.airline.parser.ParseArgumentsMissingException;
import com.github.rvesse.airline.parser.ParseOptionMissingException;

import java.util.concurrent.Callable;

import static java.util.Arrays.asList;

public class DjinniCli {
    public static void main(String... args) throws Exception {
        CliBuilder<Callable<Void>> builder = Cli.<Callable<Void>>builder("djinni")
                .withDescription("tooling for testing of legacy databases")
                .withCommands(asList(Help.class, ExportCli.class, VerifyCli.class, ImportCli.class))
                .withDefaultCommand(Help.class);

        try {
            builder.build().parse(args).call();
        } catch (ParseArgumentsMissingException | ParseOptionMissingException e) {
            System.err.println(e.getMessage());
            System.exit(3);
        }
    }
}
