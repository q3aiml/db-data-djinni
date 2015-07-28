package net.q3aiml.dbdata.verify;

import net.q3aiml.dbdata.DataSpec;

import static com.google.common.base.Preconditions.checkNotNull;

public class VerificationError {
    private Type type;
    private DataSpec.DataSpecRow actualRow;
    private DataSpec.DataSpecRow expectedRow;
    private String message;

    public VerificationError(
            Type type, DataSpec.DataSpecRow actualRow, DataSpec.DataSpecRow expectedRow, String column)
    {
        this.type = type;
        this.actualRow = actualRow;
        this.expectedRow = checkNotNull(expectedRow, "expectedRow must not be null");
        if (type == Type.MISSING_ROW) {
            message = "missing row " + expectedRow;
        } else {
            message = "mismatch on " + column + " in " + expectedRow;
        }
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
