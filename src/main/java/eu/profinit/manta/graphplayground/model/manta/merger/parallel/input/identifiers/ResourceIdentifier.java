package eu.profinit.manta.graphplayground.model.manta.merger.parallel.input.identifiers;

import java.util.Objects;

/**
 * Resource identifier.
 *
 * @author tpolacok
 */
public class ResourceIdentifier {

    /**
     * Name of resource.
     */
    private final String name;

    /**
     * Type of resource.
     */
    private final String type;

    /**
     * Description of resource.
     */
    private final String description;

    /**
     * @param name Name of resource.
     * @param type Type of resource.
     * @param description Description of resource.
     */
    public ResourceIdentifier(String name, String type, String description) {
        this.name = name;
        this.type = type;
        this.description = description;
    }

    /**
     * @return Name of resource.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Type of resource.
     */
    public String getType() {
        return type;
    }

    /**
     * @return Description of resource.
     */
    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResourceIdentifier that = (ResourceIdentifier) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(type, that.type) &&
                Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, description);
    }
}
