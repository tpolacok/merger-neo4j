package eu.profinit.manta.graphplayground.model.manta.merger;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Mapping of source code's local identifier to its database identifier.
 *
 * @author tfechtner
 */
public class SourceCodeMapping {

    /**
     * Local source code identifier.
     */
    private final String localId;

    /**
     * Database source code identifier.
     */
    private final String dbId;

    /**
     * @param localId Local source code identifier.
     * @param dbId Database source code identifier.
     */
    public SourceCodeMapping(String localId, String dbId) {
        super();
        this.localId = localId;
        this.dbId = dbId;
    }

    /**
     * @return Local source code identifier.
     */
    public String getLocalId() {
        return localId;
    }

    /**
     * @return Database source code identifier.
     */
    public String getDbId() {
        return dbId;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(localId);
        builder.append(dbId);
        return builder.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        SourceCodeMapping other = (SourceCodeMapping) obj;
        EqualsBuilder builder = new EqualsBuilder();
        builder.append(localId, other.localId);
        builder.append(dbId, other.dbId);
        return builder.isEquals();
    }

}
