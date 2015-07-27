package net.q3aiml.dbdata.yaml;

import org.yaml.snakeyaml.nodes.Node;

public abstract class AnchorGenerator {
    public abstract String generateAnchor(Node node);

    /**
     * For stateful anchor provider implementations
     */
    public void reset() { }
}
