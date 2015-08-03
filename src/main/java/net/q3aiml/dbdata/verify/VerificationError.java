package net.q3aiml.dbdata.verify;

import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.util.MoreCollectors;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class VerificationError {
    private Type type;
    private DataSpec.DataSpecRow actualRow;
    private DataSpec.DataSpecRow expectedRow;
    private String message;

    public VerificationError(
            Type type, DataSpec.DataSpecRow actualRow, DataSpec.DataSpecRow expectedRow)
    {
        this.type = type;
        this.actualRow = actualRow;
        this.expectedRow = checkNotNull(expectedRow, "expectedRow must not be null");
        if (type == Type.MISSING_ROW) {
            message = "missing row " + expectedRow;
        } else {
            Map<String, Object> differences = diffRows(actualRow, expectedRow);
            message = "mismatch values in " + actualRow.getTable() + ": " + differences
                    + " (expected: " + expectedRow.getRow() + ", actual: " + actualRow.getRow() + ")";
        }
    }

    protected static Map<String, Object> diffRows(DataSpec.DataSpecRow actualRow, DataSpec.DataSpecRow expectedRow) {
        return expectedRow.getRow().entrySet().stream()
                .filter(desiredColumn -> !Objects.equals(desiredColumn.getValue(),
                        actualRow.getRow().get(desiredColumn.getKey())))
                .collect(MoreCollectors.toMap(Map.Entry::getKey, Map.Entry::getValue, LinkedHashMap::new));
    }

    public Type type() {
        return type;
    }

    public DataSpec.DataSpecRow actualRow() {
        return actualRow;
    }

    public DataSpec.DataSpecRow expectedRow() {
        return expectedRow;
    }

    public enum Type {
        MISSING_ROW,
        VALUE_MISMATCH
    }

    @Override
    public String toString() {
        return message;
    }
}
