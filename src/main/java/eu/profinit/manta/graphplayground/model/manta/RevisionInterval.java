package eu.profinit.manta.graphplayground.model.manta;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * DTO pro označení intervalu revizí.
 * 
 * Every revision is represented by a double number. The integer part is called major, the decimal part is called minor.
 * Revision example: 1.000123 (major=1, minor=000123)
 * Revision interval example: <1.000123, 2.001058> (startRevision=1.000123, endRevision=2.001058) 
 * Decimal part takes exactly 6 digits. Hence, the maximum number of minor revisions within one major revision is exactly one million.
 * @author tfechtner
 * @author jsykora
 *
 */
public class RevisionInterval {
    /** Startovní revize intervalu. */
    private final double start;
    /** Konečná revize intervalu. */
    private final double end;

    /**
     * @param start Startovní revize intervalu.
     * @param end Konečná revize intervalu.
     */
    public RevisionInterval(double start, double end) {
        super();
        Validate.isTrue(start <= end, "Start revision must be smaller or equal than end revision.");
        this.start = start;
        this.end = end;
    }
    
    /**
     * Revision interval containing exactly one revision <revision, revision>
     * @param revision  start and end revision of the interval
     */
    public RevisionInterval(double revision) {
        super();
        this.start = revision;
        this.end = revision;
    }

    /**
     * @return Startovní revize intervalu.
     */
    public double getStart() {
        return start;
    }

    /**
     * @return Konečná revize intervalu.
     */
    public double getEnd() {
        return end;
    }

    /**
     * @return true, jestliže jde o porovnávací interval
     */
    public boolean isComparing() {
        return start != 0 && start != end;
    }

    @Override
    public String toString() {
        return "Rev[" + start + ":" + end + "]";
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(start);
        builder.append(end);
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
        RevisionInterval other = (RevisionInterval) obj;
        EqualsBuilder builder = new EqualsBuilder();
        builder.append(start, other.start);
        builder.append(end, other.end);
        return builder.isEquals();
    }

    /**
     * Vytvoří nový interval, který bude průnikem dvou zadaných.
     * @param interval1 první interval pro průnik
     * @param interval2 druhý interval pro průnik
     * @return průnikový interval nebo null, při prázdném průniku
     */
    public static RevisionInterval createIntersection(RevisionInterval interval1, RevisionInterval interval2) {
        double start = Math.max(interval1.getStart(), interval2.getStart());
        double end = Math.min(interval1.getEnd(), interval2.getEnd());

        if (start <= end) {
            return new RevisionInterval(start, end);
        } else {
            return null;
        }
    }
}
