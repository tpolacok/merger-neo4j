package eu.profinit.manta.graphplayground.model.manta.merger.parallel.input.identifiers;

import java.util.Objects;

/**
 * Layer identifier.
 *
 * @author tpolacok
 */
public class LayerIdentifier {

    /**
     * Name of layer.
     */
    private final String name;

    /**
     * Type of layer.
     */
    private final String type;

    /**
     * @param name Name of layer.
     * @param type Type of layer.
     */
    public LayerIdentifier(String name, String type) {
        this.name = name;
        this.type = type;
    }

    /**
     * @return Name of layer.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Type of layer.
     */
    public String getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LayerIdentifier that = (LayerIdentifier) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }
}
