package net.q3aiml.dbdata;

import net.q3aiml.dbdata.config.DatabaseConfig;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DataSpec {
    public List<DataSpecRow> tableRows = new ArrayList<>();

    public static DataSpec fromYaml(String serializedConfig) {
        return new Yaml().loadAs(serializedConfig, DataSpec.class);
    }

    public String toYaml(DatabaseConfig config) {
        return new Yaml().dumpAs(this, Tag.MAP, DumperOptions.FlowStyle.BLOCK);
    }

    @Override
    public String toString() {
        return "DataSpec{" +
                "tableRows=" + tableRows +
                '}';
    }

    public static class DataSpecRow {
        private String tableName;
        private Map<String, Object> rowValues = new LinkedHashMap<>();

        public String getTable() {
            return tableName;
        }

        public void setTable(String tableName) {
            this.tableName = tableName;
        }

        public Map<String, Object> getRow() {
            return rowValues;
        }

        public void setRow(Map<String, Object> rowValues) {
            this.rowValues = rowValues;
        }

        @Override
        public String toString() {
            return "DataSpecRow{" +
                    "tableName='" + tableName + '\'' +
                    ", rowValues=" + rowValues +
                    '}';
        }
    }
}
