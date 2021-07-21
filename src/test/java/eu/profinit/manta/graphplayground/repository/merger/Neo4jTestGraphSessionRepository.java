package eu.profinit.manta.graphplayground.repository.merger;

import eu.profinit.manta.graphplayground.repository.GraphSessionRepository;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;

public class Neo4jTestGraphSessionRepository implements GraphSessionRepository<Session,Transaction> {
    private final Driver driver;

    private Session session;
    private Transaction transaction;

    public Neo4jTestGraphSessionRepository(Driver driver) {
        this.driver = driver;
        this.session = driver.session();
    }

    @Override
    public void close() {
        session.close();
        driver.close();
    }

    @Override
    public Session getSession() {
        if (session == null || !session.isOpen()) {
            session = driver.session();
        }

        return session;
    }

    @Override
    public void beginTransaction() {
        if (session == null || !session.isOpen()) {
            session = driver.session();
        }

        transaction = session.beginTransaction();
    }

    @Override
    public void commit() {
        if (transaction != null && transaction.isOpen()) {
            transaction.commit();
        } else {
            throw new RuntimeException("Transaction cannot be committed");
        }
    }

    @Override
    public void rollback() {
        if (transaction != null && transaction.isOpen()) {
            transaction.rollback();
        } else {
            throw new RuntimeException("Transaction cannot be rollback");
        }
    }

    @Override
    public Transaction getActiveTransaction() {
        if (transaction == null || !transaction.isOpen()) {
            beginTransaction();
        }

        return transaction;
    }
}
