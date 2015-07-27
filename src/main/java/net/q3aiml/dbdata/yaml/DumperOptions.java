package net.q3aiml.dbdata.yaml;

public class DumperOptions extends org.yaml.snakeyaml.DumperOptions {
    private AnchorGenerator anchorGenerator = new DefaultNumericAnchorGenerator();

    public AnchorGenerator getAnchorGenerator() {
        return anchorGenerator;
    }

    public void setAnchorGenerator(AnchorGenerator anchorGenerator) {
        this.anchorGenerator = anchorGenerator;
    }
}
