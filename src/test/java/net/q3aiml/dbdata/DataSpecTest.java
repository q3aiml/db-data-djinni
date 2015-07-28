package net.q3aiml.dbdata;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import net.q3aiml.dbdata.config.DatabaseConfig;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class DataSpecTest {
    DataSpec.DataSpecRow rowTruffles = new DataSpec.DataSpecRow("user", ImmutableMap.of("name", "truffles"));
    DataSpec.DataSpecRow rowRadicchio = new DataSpec.DataSpecRow("user", ImmutableMap.of("name", "radicchio"));
    DataSpec.DataSpecRow rowPellets = new DataSpec.DataSpecRow("group", ImmutableMap.of("name", "PELLETS"));
    DataSpec.DataSpecRow rowChicory = new DataSpec.DataSpecRow("group", ImmutableMap.of("name", "chicory"));
    DataSpec.DataSpecRow rowTrufflesPellets = new DataSpec.DataSpecRow("group_user", ImmutableMap.of(
            "user_fk", rowTruffles,
            "group_fk", rowPellets
    ));
    DataSpec.DataSpecRow rowRadicchioChicory = new DataSpec.DataSpecRow("group_user", ImmutableMap.of(
            "user_fk", rowRadicchio,
            "group_fk", rowChicory
    ));
    DataSpec dataSpec = new DataSpec(asList(
            rowTruffles, rowRadicchio, rowPellets, rowChicory,rowTrufflesPellets, rowRadicchioChicory
    ));
    DatabaseConfig config = new DatabaseConfig();

    @Test
    public void toYamlWithAltKeyTest() throws IOException {
        config.rekeyer.altKeys.put("user", asList("name"));
        assertEquals(
                Resources.toString(Resources.getResource("dataspec_altkeys.yaml"), StandardCharsets.UTF_8),
                dataSpec.toYaml(config));
    }

    @Test
    public void toYamlWithoutAltKeyTest() throws IOException {
        assertEquals(
                Resources.toString(Resources.getResource("dataspec.yaml"), StandardCharsets.UTF_8),
                dataSpec.toYaml(config));
    }
}