package net.q3aiml.dbdata.yaml;

import org.yaml.snakeyaml.nodes.Node;

import java.text.NumberFormat;

public class DefaultNumericAnchorGenerator extends AnchorGenerator {
    private int lastAnchorId = 0;

    public String generateAnchor(Node node) {
        this.lastAnchorId++;
        NumberFormat format = NumberFormat.getNumberInstance();
        format.setMinimumIntegerDigits(3);
        format.setMaximumFractionDigits(0);// issue 172
        format.setGroupingUsed(false);
        String anchorId = format.format(this.lastAnchorId);
        return "id" + anchorId;
    }

    public void reset() {
        this.lastAnchorId = 0;
    }
}
