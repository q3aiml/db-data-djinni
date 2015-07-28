package net.q3aiml.dbdata.modify;

import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.config.DatabaseConfig;
import net.q3aiml.dbdata.introspect.DefaultDatabaseIntrospector;
import net.q3aiml.dbdata.jdbc.UnpreparedStatement;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.verify.VerificationError;
import net.q3aiml.dbdata.verify.Verifier;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class Importer {
    protected final DefaultDatabaseIntrospector introspector;
    protected final Verifier verifier;
    protected final DataSpecModifySql modifySql;

    public Importer(DefaultDatabaseIntrospector introspector, Verifier verifier, DataSpecModifySql modifySql) {
        this.introspector = introspector;
        this.verifier = verifier;
        this.modifySql = modifySql;
    }

    public List<UnpreparedStatement> generateChangesToApplyDataSpecToDatabase(
            Connection c, DataSpec dataSpec, DatabaseConfig config) throws SQLException
    {
        DatabaseMetadata db = new DatabaseMetadata();
        introspector.loadTables(c, db);
        introspector.loadTablePrimaryKeyInfo(c, db);
        introspector.loadTableUniqueInfo(c, db);
        introspector.loadReferences(c, db);

        List<VerificationError> verifyErrors = verifier.verify(c, dataSpec, config, db);
        return verifyErrors.stream()
                .map(verifyError -> {
                    if (verifyError.type() == VerificationError.Type.VALUE_MISMATCH) {
                        return modifySql.updateSql(verifyError.expectedRow(), db);
                    } else if (verifyError.type() == VerificationError.Type.MISSING_ROW) {
                        return modifySql.deleteSql(verifyError.expectedRow(), db);
                    } else {
                        throw new UnsupportedOperationException("unexpected verify error type " + verifyError);
                    }
                })
                .collect(toList());
    }
}
