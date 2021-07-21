package eu.profinit.manta.graphplayground.model.manta.core;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Unikátní identifikace hrany v rámci dané revizi.
 * @author tfechtner
 *
 */
public class EdgeIdentification {
    /** ID startovního vrcholu. */
    private final long startId;
    /** ID koncového vrcholu. */
    private final long endId;
    /** Label hrany. */
    private final DatabaseStructure.MantaRelationshipType label;
    /** Id relace v hrany v titanu, nepoužívá se v equals. */
    private final Long relationId;

    /**
     * @param startId Start node id
     * @param endId End node id
     * @param type Relationship's label
     * @param relationId Relationship's id
     */
    public EdgeIdentification(long startId, long endId, Long relationId, String type) {
        super();
        this.startId = startId;
        this.endId = endId;
        this.label = DatabaseStructure.MantaRelationshipType.parseFromDbType(type);
        this.relationId = relationId;
    }

    /**
     * @return ID startovního vrcholu
     */
    public long getStartId() {
        return startId;
    }

    /**
     * @return ID koncového vrcholu
     */
    public long getEndId() {
        return endId;
    }

    /**
     * @return Label hrany
     */
    public DatabaseStructure.MantaRelationshipType getLabel() {
        return label;
    }

    /**
     * @return Id relace v hrany v titanu, nepoužívá se v equals.
     */
    public Long getRelationId() {
        return relationId;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(startId);
        builder.append(endId);
        builder.append(label);
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
        EdgeIdentification other = (EdgeIdentification) obj;
        EqualsBuilder builder = new EqualsBuilder();
        builder.append(startId, other.startId);
        builder.append(endId, other.endId);
        builder.append(label, other.label);
        return builder.isEquals();
    }

    @Override
    public String toString() {
        return startId + "-" + label + "->" + endId + "[" + relationId + "]";
    }
}