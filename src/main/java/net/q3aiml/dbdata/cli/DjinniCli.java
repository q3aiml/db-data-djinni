package net.q3aiml.dbdata.cli;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.builder.CliBuilder;
import com.github.rvesse.airline.help.Help;

import java.util.concurrent.Callable;

import static java.util.Arrays.asList;

public class DjinniCli {
    public static void main(String... args) throws Exception {
        CliBuilder<Callable<Void>> builder = Cli.<Callable<Void>>builder("magic")
                .withDescription("tooling for testing of legacy databases")
                .withCommands(asList(Help.class, ExportCli.class, VerifyCli.class))
                .withDefaultCommand(Help.class);
        builder.build().parse(args).call();
    }
}
