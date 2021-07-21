package eu.profinit.manta.graphplayground.repository;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Repository;

import java.net.URI;

@Repository
@Primary
public class Neo4jGraphSessionRepository implements GraphSessionRepository<Session, Transaction> {
    private final static URI NEO4J_URL = URI.create("bolt://localhost:7687");
    private final static String NEO4J_USERNAME = "neo4j";
    private final static String NEO4J_PASSWORD = "admin";

    private final Driver driver;
    private Session session;
    private Transaction transaction;

    public Neo4jGraphSessionRepository() {
        Config config = Config.builder()
                .build();

        this.driver = GraphDatabase.driver(NEO4J_URL, AuthTokens.basic(NEO4J_USERNAME, NEO4J_PASSWORD), config);
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
        transaction = session.beginTransaction();
    }

    @Override
    public void commit() {
        if (transaction != null && transaction.isOpen()) {
            transaction.commit();
        }
    }

    @Override
    public void rollback() {
        if (transaction != null && transaction.isOpen()) {
            transaction.rollback();
        }
    }

    @Override
    public Transaction getActiveTransaction() {
        if (transaction != null && transaction.isOpen()) {
            return transaction;
        }

        throw new RuntimeException("Transaction is not open");
    }
}
