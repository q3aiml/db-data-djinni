package net.q3aiml.dbdata.config;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableMap;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.model.ForeignKeyReference;
import net.q3aiml.dbdata.model.Table;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

public class DatabaseConfig {
    public static DatabaseConfig fromYaml(String yaml, DatabaseMetadata db) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.setInjectableValues(new InjectableValues.Std(ImmutableMap.of(
                "metadata", db
        )));
        return mapper.readValue(yaml, DatabaseConfig.class);
    }

    @JacksonInject("metadata")
    public DatabaseMetadata databaseMetadata;

    @JsonProperty("schema")
    public String schema;

    @JsonProperty("tables")
    public Map<String, DatabaseTableConfig> tableConfigs = new HashMap<>();

    @JsonProperty("global")
    public DatabaseTableConfig globalConfig = new DatabaseTableConfig();

    @JsonProperty("rekeyer")
    public RekeyerConfig rekeyer = new RekeyerConfig();

    @JsonProperty("foreign_keys")
    public List<ForeignKeyReference> foreignKeys = new ArrayList<>();

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
