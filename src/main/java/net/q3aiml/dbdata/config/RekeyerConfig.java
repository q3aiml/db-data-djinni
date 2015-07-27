package net.q3aiml.dbdata.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RekeyerConfig {
    @JsonProperty("alt_keys")
    public Map<String, List<String>> altKeys = new HashMap<>();
}
