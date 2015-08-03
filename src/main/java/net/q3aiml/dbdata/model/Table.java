package net.q3aiml.dbdata.model;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Strings.isNullOrEmpty;

public class Table {
    public String schema;
    public String name;
    private UniqueInfo uniqueInfo = new UniqueInfo();
    private List<String> primaryKeyColumns;

    public Table(String schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    public String fullName() {
        return isNullOrEmpty(schema) ? name : schema + "." + name;
    }

    public List<String> primaryKeyColumns() {
        return primaryKeyColumns;
    }

    public void setPrimaryKeyColumns(List<String> primaryKeyColumns) {
        this.primaryKeyColumns = primaryKeyColumns;
    }

    public UniqueInfo uniqueInfo() {
        return uniqueInfo;
    }

    public void addUniqueInfo(UniqueInfo uniqueInfo) {
        this.uniqueInfo.add(uniqueInfo);
    }

    @Override
    public String toString() {
        return "Table{" +
                "schema='" + schema + '\'' +
                ", name='" + name + '\'' +
                ", uniqueInfo=" + uniqueInfo +
                '}';
    }
}
