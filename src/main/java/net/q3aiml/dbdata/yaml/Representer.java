package net.q3aiml.dbdata.yaml;

import org.yaml.snakeyaml.nodes.Node;

import java.util.Map;

public class Representer extends org.yaml.snakeyaml.representer.Representer {
    private RepresenterListener defaultRepresenterListener;

    public RepresenterListener getDefaultRepresenterListener() {
        return defaultRepresenterListener;
    }

    public void setDefaultRepresenterListener(RepresenterListener defaultRepresenterListener) {
        this.defaultRepresenterListener = defaultRepresenterListener;
    }

    public Node represent(Object data) {
        return represent(data, defaultRepresenterListener);
    }

    public Node represent(Object data, RepresenterListener listener) {
        Node node = representData(data);
        if (listener != null) {
            listener.representedObjects(representedObjects);
        }
        representedObjects.clear();
        objectToRepresent = null;
        return node;
    }

    public interface RepresenterListener {
        void representedObjects(Map<Object, Node> representedObjects);
    }
}
