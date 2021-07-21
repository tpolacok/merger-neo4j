package eu.profinit.manta.graphplayground.repository.merger.stored;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.MantaRelationshipType;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.EdgeFormat;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.NodeFormat;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.ResourceFormat;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.merger.connector.GraphOperation;
import eu.profinit.manta.graphplayground.repository.merger.connector.SourceRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.connector.SuperRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedEdgeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedNodeAttributeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedNodeResourceEdgeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedPerspectiveEdgeQuery;
import eu.profinit.manta.graphplayground.repository.merger.revision.RevisionRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphCreation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphOperation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSourceRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSuperRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.util.Map;

import static eu.profinit.manta.graphplayground.repository.merger.common.CommonMergerTestObjects.*;

/**
 * Delayed queries evaluation tests.
 *
 * @author tpolacok
 */
public class DelayedQueryTest {

    /**
     * Driver to the Neo4j.
     */
    protected static Driver driver;

    /**
     * Neo4j service.
     */
    protected static Neo4j neo4j;

    /**
     * Standard merger processor
     */
    protected static StoredMergerProcessor testedProcessor;

    /**
     * Standard processor context.
     */
    protected static StoredProcessorContext context;

    /**
     * Utility methods for testing.
     */
    protected static StoredTestGraphUtil testGraphUtil;


    @BeforeAll
    static void initDatabase() {
        neo4j = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .build();
        driver = GraphDatabase.driver(neo4j.boltURI(), AuthTokens.none());
    }

    @BeforeEach
    void cleanAndInitGraph() {
        // Init database roots
        RevisionRootHandler revisionRootHandler = new RevisionRootHandler();
        Double mergedRevision = driver.session().writeTransaction(tx -> {
            tx.run("MATCH (n) DETACH DELETE (n)");
            new SuperRootHandler().ensureRootExistance(tx);
            revisionRootHandler.ensureRootExistance(tx);
            new SourceRootHandler(new GraphOperation()).ensureRootExistance(tx);
            revisionRootHandler.ensureRootExistance(tx);
            // Technical revision
            Double rev = revisionRootHandler.createMajorRevision(tx);
            revisionRootHandler.commitRevision(tx, rev);
            return revisionRootHandler.createMajorRevision(tx);
        });
        Double latestCommittedRevision = driver.session()
                .writeTransaction(revisionRootHandler::getLatestCommittedRevisionNumber);

        testGraphUtil = new StoredTestGraphUtil(driver, neo4j.defaultDatabaseService());
        // Init merger processor
        StoredRevisionUtils revisionUtils = new StoredRevisionUtils();
        StoredGraphOperation graphOperation = new StoredGraphOperation(revisionUtils);
        StoredGraphCreation graphCreation = new StoredGraphCreation(graphOperation);
        testedProcessor = new StoredMergerProcessor(
                new StoredSuperRootHandler(), new StoredSourceRootHandler(graphOperation), new StoredRevisionRootHandler(),
                graphOperation, graphCreation, revisionUtils, StoredMergerProcessorTest.LOGGER);

        context = new StoredProcessorContext(mergedRevision, latestCommittedRevision);
    }

    @Test
    public void testEvaluateDelayedNodeAttributeQuery() {
        String key = "attributeKey";
        String value = "attributeValue";
        testGraphUtil.merge(testedProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(testedProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(testedProcessor, context, MERGED_NODE_RESOURCE);
        final DelayedNodeAttributeQuery nodeAttributeQuery =
                new DelayedNodeAttributeQuery(
                    context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]),
                    key,
                    value,
                    REVISION_INTERVAL
                );
        testGraphUtil.merge(testedProcessor, context, nodeAttributeQuery);

        Long nodeId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]);

        Map<String, Object> nodeResult = testGraphUtil.getNodeAttribute(nodeId, key, value);
        testGraphUtil.compareNodeAttribute(new String[]{"", "", key, value}, (Node)nodeResult.get("attr"));
        testGraphUtil.compareRelationshipRevision((Relationship)nodeResult.get("rel"), REVISION_INTERVAL);
    }

    @Test
    public void testEvaluateDelayedNodeResourceEdgeQuery() {
        testGraphUtil.merge(testedProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(testedProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(testedProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(testedProcessor, context, MERGED_NODE_PARENT);
        final DelayedNodeResourceEdgeQuery resourceEdgeQuery =
                new DelayedNodeResourceEdgeQuery(
                    MERGED_NODE_PARENT[NodeFormat.ID.getIndex()],
                    MERGED_RESOURCE[ResourceFormat.ID.getIndex()]
                );
        testGraphUtil.merge(testedProcessor, context, resourceEdgeQuery);
        Long nodeId = context.getMapNodeIdToDbId().get(MERGED_NODE_PARENT[NodeFormat.ID.getIndex()]);;
        Long resourceId = context.getMapResourceIdToDbId().get(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]);
        Map<String, Object> result = testGraphUtil.getRelationship(nodeId, resourceId, MantaRelationshipType.HAS_RESOURCE);
        testGraphUtil.compareRelationshipRevision((Relationship)result.get("rel"), REVISION_INTERVAL);
    }

    @Test
    public void testEvaluateDelayedPerspectiveEdgeQuery() {
        String perspectiveKey = "perspectiveKey";
        String perspectiveValue = "perspectiveValue";
        Map<String, Object> edgeAttributes = Map.of("layer", "Logical");
        testGraphUtil.merge(testedProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(testedProcessor, context, MERGED_LAYER_2);
        testGraphUtil.merge(testedProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(testedProcessor, context, MERGED_RESOURCE_2);
        testGraphUtil.merge(testedProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(testedProcessor, context, MERGED_NODE_RESOURCE_2);
        final DelayedPerspectiveEdgeQuery perspectiveEdgeQuery =
                new DelayedPerspectiveEdgeQuery(
                    "",
                    context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]),
                    context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE_2[NodeFormat.ID.getIndex()]),
                    MantaRelationshipType.PERSPECTIVE,
                    REVISION_INTERVAL,
                    edgeAttributes,
                    new DelayedNodeAttributeQuery(
                            context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]),
                            perspectiveKey,
                            perspectiveValue,
                            REVISION_INTERVAL
                    )
                );
        testGraphUtil.merge(testedProcessor, context, perspectiveEdgeQuery);

        Long sourceId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]);
        Long targetId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE_2[NodeFormat.ID.getIndex()]);

        Map<String, Object> edgeResult = testGraphUtil.getRelationship(sourceId, targetId, MantaRelationshipType.PERSPECTIVE);
        Map<String, Object> nodeAttributeResult = testGraphUtil.getAllNodeAttributes(sourceId);

        testGraphUtil.compareNodeAttribute(
                new String[]{"", "", perspectiveKey, perspectiveValue},
                (Node)nodeAttributeResult.get("attr"));
        testGraphUtil.compareRelationshipRevision((Relationship)nodeAttributeResult.get("rel"), REVISION_INTERVAL);
        testGraphUtil.compareRelationship((Relationship)edgeResult.get("rel"), REVISION_INTERVAL,
                MantaRelationshipType.PERSPECTIVE, edgeAttributes);
    }

    @Test
    public void testEvaluateDelayedEdgeQuery() {
        testGraphUtil.merge(testedProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(testedProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(testedProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(testedProcessor, context, MERGED_NODE_PARENT);
        final DelayedEdgeQuery delayedEdgeQuery =
                new DelayedEdgeQuery(
                        MERGED_EDGE[EdgeFormat.ID.getIndex()],
                        context.getMapNodeIdToDbId().get(MERGED_EDGE[EdgeFormat.SOURCE.getIndex()]),
                        context.getMapNodeIdToDbId().get(MERGED_EDGE[EdgeFormat.TARGET.getIndex()]),
                        MantaRelationshipType.DIRECT,
                        REVISION_INTERVAL,
                        Map.of()
                );
        testGraphUtil.merge(testedProcessor, context, delayedEdgeQuery);

        Map<String, Object> edgeResult =
                testGraphUtil.getRelationship(context.getMapEdgeIdToDbId().get(MERGED_EDGE[EdgeFormat.ID.getIndex()]));
        testGraphUtil.compareRelationship((Relationship)edgeResult.get("rel"), REVISION_INTERVAL,
                MantaRelationshipType.DIRECT, Map.of());
    }
}
