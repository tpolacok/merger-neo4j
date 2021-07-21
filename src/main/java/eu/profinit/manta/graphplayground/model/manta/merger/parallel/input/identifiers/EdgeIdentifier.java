package eu.profinit.manta.graphplayground.model.manta.merger.parallel.input.identifiers;

import java.util.Objects;

import org.neo4j.graphdb.Direction;

/**
 * Edge identifier.
 *
 * @author tpolacok.
 */
public class EdgeIdentifier {

    /**
     * Type of edge.
     */
    private final String type;

    /**
     * Edge direction.
     */
    private final Direction direction;

    /**
     * @param type Type of edge.
     * @param direction Edge direction.
     */
    public EdgeIdentifier(String type, Direction direction) {
        this.type = type;
        this.direction = direction;
    }

    /**
     * @return Edge type.
     */
    public String getType() {
        return type;
    }

    /**
     * @return Edge direction.
     */
    public Direction getDirection() {
        return direction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EdgeIdentifier that = (EdgeIdentifier) o;
        return Objects.equals(type, that.type) &&
                direction == that.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, direction);
    }
}
