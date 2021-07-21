package eu.profinit.manta.graphplayground.repository.merger.revision;

/**
 * Exception thrown in case of problem with metadata revisions.
 * e.g. Committing of committed revision.
 *
 * @author tfechtner
 */
public class RevisionException extends RuntimeException {

    private static final long serialVersionUID = -7038959416462821973L;

    /**
     * @param message Text of the exception.
     * @param cause Cause of the exception.
     */
    public RevisionException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message Text of the exception.
     */
    public RevisionException(String message) {
        super(message);
    }

}
