package eu.profinit.manta.graphplayground.model.manta.merger.parallel.input.identifiers;

import java.util.Objects;

/**
 * Edge attribute identifier.
 */
public class EdgeAttributeIdentifier {

    /**
     * Name of attribute.
     */
    private final String name;

    /**
     * Value of attribute.
     */
    private final String value;

    /**
     * @param name Name of attribute.
     * @param value Value of attribute.
     */
    public EdgeAttributeIdentifier(String name, String value) {
        this.name = name;
        this.value = value;
    }

    /**
     * @return Name of attribute.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Value of attribute.
     */
    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EdgeAttributeIdentifier that = (EdgeAttributeIdentifier) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }
}
