package eu.profinit.manta.graphplayground.model.manta.merger.parallel.input.identifiers;

import java.util.Objects;

/**
 * Source code identifier.
 *
 * @author tpolacok
 */
public class SourceCodeIdentifier {

    /**
     * Location of the source code.
     */
    private final String path;

    /**
     * Hash for the source code.
     */
    private final String hash;

    /**
     * Technology of the source code.
     */
    private final String technology;

    /**
     * Connection of the source code.
     */
    private final String connection;

    public SourceCodeIdentifier(String path, String hash, String technology, String connection) {
        this.path = path;
        this.hash = hash;
        this.technology = technology;
        this.connection = connection;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SourceCodeIdentifier that = (SourceCodeIdentifier) o;
        return Objects.equals(path, that.path) && Objects.equals(hash, that.hash) && Objects.equals(technology, that.technology) && Objects.equals(connection, that.connection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, hash, technology, connection);
    }
}
