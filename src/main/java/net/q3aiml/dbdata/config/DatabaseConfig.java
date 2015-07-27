package net.q3aiml.dbdata.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
    public RekeyerConfig rekeyer;

    public boolean isIgnored(String columnName) {
        return globalConfig.omitColumns.stream().map(String::toLowerCase).anyMatch(s -> s.equals(columnName.toLowerCase()));
    }

    public static class DatabaseTableConfig {
        @JsonProperty("omit_columns")
        public Set<String> omitColumns = new HashSet<>();
    }
}
