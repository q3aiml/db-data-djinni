package net.q3aiml.dbdata.model;

public class Table {
    public String schema;
    public String name;

    public Table(String schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    @Override
    public String toString() {
        return "Table{" +
                "schema='" + schema + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
