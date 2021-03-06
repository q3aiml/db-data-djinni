package net.q3aiml.dbdata;

import com.google.common.collect.*;
import net.q3aiml.dbdata.config.DatabaseConfig;
import net.q3aiml.dbdata.config.RekeyerConfig;
import net.q3aiml.dbdata.util.MoreCollectors;
import net.q3aiml.dbdata.yaml.*;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;
import org.yaml.snakeyaml.nodes.AnchorNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.Tag;

import java.beans.IntrospectionException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

public class DataSpec {
    public List<DataSpecRow> tableRows = new ArrayList<>();

    public DataSpec() { }

    public DataSpec(List<DataSpecRow> tableRows) {
        this.tableRows = tableRows;
    }

    public static DataSpec fromYaml(String serializedConfig) {
        return new Yaml().loadAs(serializedConfig, DataSpec.class);
    }

    public String toYaml(DatabaseConfig config) {
        // table coming first seems more readable
        ImmutableMultimap<String, String> propertyOrders = ImmutableMultimap.<String, String>builder()
                .put(DataSpecRow.class.getName(), "table")
                .put(DataSpecRow.class.getName(), "row")
                .build();

        Representer representer = new Representer();
        representer.setPropertyUtils(new PropertyUtils() {
            @Override
            protected Set<Property> createPropertySet(Class<?> type, BeanAccess bAccess) throws IntrospectionException {
                Set<Property> propertySet = super.createPropertySet(type, bAccess);
                List<String> propertyOrder = propertyOrders.get(type.getName()).asList();
                if (propertyOrder != null) {
                    Ordering<Property> propertyOrdering = Ordering.explicit(propertyOrder)
                            .onResultOf(Property::getName);
                    return propertySet.stream().sorted(propertyOrdering)
                            .collect(Collectors.toCollection(LinkedHashSet::new));
                } else {
                    return propertySet;
                }
            }
        });
        DumperOptions options = new DumperOptions();
        if (config != null) {
            FancyAnchorGenerator anchorGenerator = new FancyAnchorGenerator(config.rekeyer);
            representer.setDefaultRepresenterListener(anchorGenerator);
            options.setAnchorGenerator(anchorGenerator);
        }

        return new Yaml(representer, options).dumpAs(this, Tag.MAP, DumperOptions.FlowStyle.BLOCK);
    }

    private static class FancyAnchorGenerator extends AnchorGenerator implements Representer.RepresenterListener {
        private final RekeyerConfig config;
        private Map<Node, Object> nodeToObject;
        private AnchorGenerator fallbackGenerator = new DefaultNumericAnchorGenerator();
        private Multiset<String> anchors = HashMultiset.create();

        private FancyAnchorGenerator(RekeyerConfig config) {
            this.config = config;
        }

        @Override
        public String generateAnchor(Node node) {
            if (nodeToObject == null) {
                throw new IllegalStateException("expected representedObjects first");
            }
            Object o = nodeToObject.get(node);
            if (o instanceof DataSpecRow) {
                DataSpecRow row = (DataSpecRow)o;
                String tableNoSchema = row.getTable().replaceAll(".*\\.", "");
                String rowKey = rowKey(row);
                return ensureUnique((tableNoSchema + (rowKey.isEmpty() ? "" :  "-" + rowKey)).toLowerCase());
            }
            return fallbackGenerator.generateAnchor(node);
        }

        private String rowKey(DataSpecRow row) {
            List<String> altKeys = config.altKeys.get(row.getTable());
            if (altKeys != null && !altKeys.isEmpty()) {
                return altKeys.stream()
                        .map(row.getRow()::get)
                        .map(v -> {
                            if (v instanceof DataSpecRow) {
                                return rowKey((DataSpecRow)v);
                            } else {
                                return v.toString();
                            }
                        })
                        .collect(joining("-")).replaceAll("\\W+", "-");
            } else {
                return "";
            }
        }

        private String ensureUnique(String anchorBase) {
            int previousCount = anchors.add(anchorBase, 1);
            if (previousCount == 0) {
                return anchorBase;
            } else {
                return anchorBase + "-" + (previousCount + 1);
            }
        }

        @Override
        public void representedObjects(Map<Object, Node> representedObjects) {
            nodeToObject = representedObjects.entrySet().stream()
                    .collect(MoreCollectors.toMap(e -> {
                        if (e.getValue() instanceof AnchorNode) {
                            return ((AnchorNode) e.getValue()).getRealNode();
                        }
                        return e.getValue();
                    }, Map.Entry::getKey));
        }

        @Override
        public void reset() {
            nodeToObject = null;
            anchors.clear();
            fallbackGenerator.reset();
        }
    }

    @Override
    public String toString() {
        return "DataSpec{" +
                "tableRows=" + tableRows +
                '}';
    }

    public static class DataSpecRow {
        public DataSpecRow() { }

        public DataSpecRow(String tableName, Map<String, Object> rowValues) {
            this.tableName = tableName;
            this.rowValues = rowValues;
        }

        public static DataSpecRow copyOfResultSetAsStrings(String tableName, ResultSet rs) throws SQLException {
            ResultSetMetaData metaData = rs.getMetaData();
            // must be a null friendly collection
            Map<String, Object> values = new LinkedHashMap<>();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                values.put(metaData.getColumnName(i), rs.getString(i));
            }
            return new DataSpecRow(tableName, values);
        }

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
