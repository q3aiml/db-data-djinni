package net.q3aiml.dbdata.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.MoreObjects;

import java.io.IOException;
import java.util.Map;

public class SqlConnectionInfo {
    public static SqlConnectionInfo fromYaml(String s) throws IOException {
        return new ObjectMapper(new YAMLFactory()).readValue(s, SqlConnectionInfo.class);
    }

    @JsonProperty("type")
    public String type;

    @JsonProperty("props")
    public Map<String, String> props;

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("type", type)
                .add("props", props.entrySet().stream().filter(e -> !e.getKey().toLowerCase().contains("password")))
                .toString();
    }
}
