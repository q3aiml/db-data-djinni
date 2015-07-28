package net.q3aiml.dbdata.morph;

import com.google.common.collect.ImmutableMap;
import net.q3aiml.dbdata.DataSpec;
import net.q3aiml.dbdata.model.DatabaseMetadata;
import net.q3aiml.dbdata.model.ForeignKeyReference;
import net.q3aiml.dbdata.model.Table;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class KeyResolverTest {
    @Test
    public void toReferencesTest() {
        DatabaseMetadata db = new DatabaseMetadata();
        Table referredTo = db.addTable(null, "REFERRED_TO");
        Table referringTo = db.addTable(null, "REFERRING_TO");
        db.addReference(new ForeignKeyReference(referredTo, asList("ID"), referringTo, asList("REF_ID")));
        // should be able to have multiple foreign keys pointing to the same thing without blowups
        db.addReference(new ForeignKeyReference(referredTo, asList("ID"), referringTo, asList("REF2_ID")));

        DataSpec config = new DataSpec();

        DataSpec.DataSpecRow referredToRow = new DataSpec.DataSpecRow();
        referredToRow.setTable("REFERRED_TO");
        referredToRow.setRow(ImmutableMap.of("ID", "5"));
        config.tableRows.add(referredToRow);

        DataSpec.DataSpecRow referringToRow = new DataSpec.DataSpecRow();
        referringToRow.setTable("REFERRING_TO");
        referringToRow.setRow(ImmutableMap.of("REF_ID", "5"));
        config.tableRows.add(referringToRow);
        DataSpec.DataSpecRow referringToRow2 = new DataSpec.DataSpecRow();
        referringToRow2.setTable("REFERRING_TO");
        // ensure multiple rows can refer to the one without blowups
        referringToRow2.setRow(ImmutableMap.of(
                "REF_ID", "5",
                "REF2_ID", "5"
        ));
        config.tableRows.add(referringToRow2);

        KeyResolver keyResolver = new KeyResolver();
        keyResolver.toReferences(config, db);

        assertEquals(referredToRow, referringToRow.getRow().get("REF_ID"));
        assertEquals(referredToRow, referringToRow2.getRow().get("REF_ID"));
        assertEquals(referredToRow, referringToRow2.getRow().get("REF2_ID"));

        keyResolver.toKeys(config, db);

        assertEquals("5", referringToRow.getRow().get("REF_ID"));
        assertEquals("5", referringToRow2.getRow().get("REF_ID"));
        assertEquals("5", referringToRow2.getRow().get("REF2_ID"));
    }
}