package eu.profinit.manta.graphplayground.repository;

/**
 * Hold/Creates/Closes Database Session
 *
 * @param <T> Session object
 * @author dbucek
 */
public interface GraphSessionRepository<S, T> extends AutoCloseable {
    S getSession();

    void beginTransaction();

    void commit();

    void rollback();

    T getActiveTransaction();
}
