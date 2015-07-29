package net.q3aiml.dbdata.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import net.q3aiml.dbdata.model.Table;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class DatabaseConfig {
    public static DatabaseConfig fromYaml(String yaml) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(yaml, DatabaseConfig.class);
    }

    @JsonProperty("schema")
    public String schema;

    @JsonProperty("tables")
    public Map<String, DatabaseTableConfig> tableConfigs;

    @JsonProperty("global")
    public DatabaseTableConfig globalConfig;

    @JsonProperty("rekeyer")
    public RekeyerConfig rekeyer = new RekeyerConfig();

    public boolean isIgnored(Table table, String columnName) {
        Stream<String> omitColumns = globalConfig.omitColumns.stream();
        DatabaseTableConfig tableConfig = tableConfigs.get(table.schema + "." + table.name);
        if (tableConfig != null && tableConfig.omitColumns != null) {
            omitColumns = Stream.concat(omitColumns, tableConfig.omitColumns.stream());
        }
        return omitColumns.map(String::toLowerCase).anyMatch(s -> s.equals(columnName.toLowerCase()));
    }

    public static class DatabaseTableConfig {
        @JsonProperty("omit_columns")
        public Set<String> omitColumns = new HashSet<>();
    }
}
