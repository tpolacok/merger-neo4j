package eu.profinit.manta.graphplayground.model.manta.merger.parallel.input.identifiers;

import java.util.Objects;

/**
 * Node attribute identifier.
 *
 * @author tpolacok
 */
public class NodeAttributeIdentifier {

    /**
     * Name of node attribute.
     */
    private final String name;

    /**
     * Value of node attribute.
     */
    private final String value;

    /**
     * @param name Name of node attribute.
     * @param value Value of node attribute.
     */
    public NodeAttributeIdentifier(String name, String value) {
        this.name = name;
        this.value = value;
    }

    /**
     * @return Name of node attribute.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Value of node attribute.
     */
    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeAttributeIdentifier that = (NodeAttributeIdentifier) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }
}
