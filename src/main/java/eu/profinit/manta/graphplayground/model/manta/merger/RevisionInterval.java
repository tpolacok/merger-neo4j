package eu.profinit.manta.graphplayground.model.manta.merger;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Class representing revision interval (start, end)
 */
public class RevisionInterval {

    /**
     * Starting revision.
     */
    private final double start;

    /**
     * Ending revision.
     */
    private final double end;

    /**
     * @param start Starting revision.
     * @param end Ending revision.
     */
    public RevisionInterval(double start, double end) {
        Validate.isTrue(start <= end, "Start revision must be smaller or equal than end revision.", new Object[0]);
        this.start = start;
        this.end = end;
    }

    /**
     * @param revision Starting and ending revision.
     */
    public RevisionInterval(double revision) {
        this.start = revision;
        this.end = revision;
    }

    public double getStart() {
        return this.start;
    }

    public double getEnd() {
        return this.end;
    }

    public boolean isComparing() {
        return this.start != 0.0D && this.start != this.end;
    }

    public String toString() {
        return "Rev[" + this.start + ":" + this.end + "]";
    }

    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(this.start);
        builder.append(this.end);
        return builder.toHashCode();
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (obj.getClass() != this.getClass()) {
            return false;
        } else {
            RevisionInterval other = (RevisionInterval)obj;
            EqualsBuilder builder = new EqualsBuilder();
            builder.append(this.start, other.start);
            builder.append(this.end, other.end);
            return builder.isEquals();
        }
    }

    public static RevisionInterval createIntersection(RevisionInterval interval1, RevisionInterval interval2) {
        double start = Math.max(interval1.getStart(), interval2.getStart());
        double end = Math.min(interval1.getEnd(), interval2.getEnd());
        return start <= end ? new RevisionInterval(start, end) : null;
    }
}