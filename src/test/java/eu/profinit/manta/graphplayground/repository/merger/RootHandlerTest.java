package eu.profinit.manta.graphplayground.repository.merger;

import eu.profinit.manta.graphplayground.repository.merger.connector.GraphOperation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.types.Node;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.profinit.manta.graphplayground.repository.merger.connector.SourceRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.connector.SuperRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.revision.RevisionRootHandler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author tpolacok
 */
public class RootHandlerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RootHandlerTest.class);

    /**
     * Driver to the Neo4j.
     */
    private static Driver driver;

    /**
     * Super root handler.
     */
    private static SuperRootHandler superRootHandler;

    /**
     * Revision root handler.
     */
    private static RevisionRootHandler revisionRootHandler;

    /**
     * Source root handler.
     */
    private static SourceRootHandler sourceRootHandler;


    @BeforeAll
    static void init() {
        Neo4j neo4j = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .build();
        driver = GraphDatabase.driver(neo4j.boltURI(), AuthTokens.none());
        superRootHandler = new SuperRootHandler();
        revisionRootHandler = new RevisionRootHandler();
        sourceRootHandler = new SourceRootHandler(new GraphOperation());
    }

    @Test
    public void testGetSuperRootNotExisting() {
        Node superRootNode = driver.session().writeTransaction(tx -> superRootHandler.ensureRootExistance(tx));
        assertThat("The super root node should be created", superRootNode, notNullValue());
    }

    @Test
    public void testGetSuperRootExisting() {
        Node superRootNode = driver.session().writeTransaction(tx -> superRootHandler.ensureRootExistance(tx));
        assertThat("The super root node should be created", superRootNode, notNullValue());

        Node superRootNode2 = driver.session().writeTransaction(tx -> superRootHandler.ensureRootExistance(tx));
        assertEquals(superRootNode.id(), superRootNode2.id(), "Only one super root node can exist");
    }

    @Test
    public void testGetSourceRootNotExisting() {
        Node sourceRootNode = driver.session().writeTransaction(tx -> sourceRootHandler.ensureRootExistance(tx));
        assertThat("The super root node should be created", sourceRootNode, notNullValue());
    }

    @Test
    public void testGetSourceRootExisting() {
        Node sourceRootNode = driver.session().writeTransaction(tx -> sourceRootHandler.ensureRootExistance(tx));
        assertThat("The super root node should be created", sourceRootNode, notNullValue());

        Node sourceRootNode2 = driver.session().writeTransaction(tx -> sourceRootHandler.ensureRootExistance(tx));
        assertEquals(sourceRootNode2.id(), sourceRootNode2.id(), "Only one super root node can exist");
    }

    @Test
    public void testGetRevisionRootNotExisting() {
        Node revisionRoot = driver.session().writeTransaction(tx -> revisionRootHandler.ensureRootExistance(tx));
        assertThat("The super root node should be created", revisionRoot, notNullValue());
    }

    @Test
    public void testGetRevisionRootExisting() {
        Node revisionRootNode = driver.session().writeTransaction(tx -> revisionRootHandler.ensureRootExistance(tx));
        assertThat("The super root node should be created", revisionRootNode, notNullValue());

        Node revisionRootNode2 = driver.session().writeTransaction(tx -> revisionRootHandler.ensureRootExistance(tx));
        assertEquals(revisionRootNode2.id(), revisionRootNode2.id(), "Only one super root node can exist");
    }

    //TODO more revision root methods
}
