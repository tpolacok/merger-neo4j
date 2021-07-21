package eu.profinit.manta.graphplayground.model.manta.merger;

import java.util.Objects;

/**
 * Class representing merging group.
 * Only instances of same merging group can be unified before merged to database.
 *
 * @author tpolacok
 */
public class MergingGroup {

    /**
     * Type of merger processor.
     */
    private MergerType mergerType;

    /**
     * Merged revision.
     */
    private double revision;

    /**
     * @param mergerType Type of merger processor.
     * @param revision Merged revision.
     */
    public MergingGroup(MergerType mergerType, double revision) {
        this.mergerType = mergerType;
        this.revision = revision;
    }

    /**
     * @return Type of merger processor.
     */
    public MergerType getMergerType() {
        return mergerType;
    }

    /**
     * @return Merged revision.
     */
    public double getRevision() {
        return revision;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MergingGroup that = (MergingGroup) o;
        return Double.compare(that.revision, revision) == 0 &&
                mergerType == that.mergerType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mergerType, revision);
    }
}
