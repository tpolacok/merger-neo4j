package eu.profinit.manta.graphplayground.model.manta.merger.parallel.input.identifiers;

import java.util.Objects;

/**
 * Node child identifier.
 *
 * @author tpolacok.
 */
public class NodeChildIdentifier {

    /**
     * Name of child.
     */
    private final String name;

    /**
     * Type of child.
     */
    private final String type;

    /**
     * @param name Name of child.
     * @param type Type of child.
     */
    public NodeChildIdentifier(String name, String type) {
        this.name = name;
        this.type = type;
    }

    /**
     * @return Name of child.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Type of child.
     */
    public String getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeChildIdentifier that = (NodeChildIdentifier) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }
}
