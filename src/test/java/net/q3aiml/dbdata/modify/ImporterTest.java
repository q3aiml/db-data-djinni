package net.q3aiml.dbdata.modify;

import com.google.common.collect.ImmutableList;
import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.config.DatabaseConfig;
import net.q3aiml.dbdata.introspect.DefaultDatabaseIntrospector;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.verify.VerificationError;
import net.q3aiml.dbdata.verify.Verifier;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class ImporterTest {
    DefaultDatabaseIntrospector introspector = mock(DefaultDatabaseIntrospector.class);
    Verifier verifier = mock(Verifier.class);
    DataSpecModifySql modifySql = mock(DataSpecModifySql.class);
    Importer importer = new Importer(introspector, verifier, modifySql);

    Connection c = mock(Connection.class);
    DataSpec dataSpec = mock(DataSpec.class);
    DatabaseConfig config = mock(DatabaseConfig.class);

    @Test
    public void test() throws SQLException {
        DataSpec.DataSpecRow missingRow = mock(DataSpec.DataSpecRow.class, "missing row");
        DataSpec.DataSpecRow mismatchRow = mock(DataSpec.DataSpecRow.class, "mismatch row");
        DataSpec.DataSpecRow mismatchActualRow = mock(DataSpec.DataSpecRow.class);
        VerificationError missingRowError = new VerificationError(VerificationError.Type.MISSING_ROW, null, missingRow);
        VerificationError valueMismatchError = new VerificationError(VerificationError.Type.VALUE_MISMATCH,
                mismatchActualRow, mismatchRow);
        when(verifier.verify(eq(c), eq(dataSpec), eq(config), any(DatabaseMetadata.class)))
                .thenReturn(ImmutableList.of(missingRowError, valueMismatchError));

        importer.generateChangesToApplyDataSpecToDatabase(c, dataSpec, config);

        verify(modifySql).insertSql(eq(missingRow), any(DatabaseMetadata.class));
        verify(modifySql).updateSql(eq(mismatchRow), eq(mismatchActualRow), any(DatabaseMetadata.class));
    }

}