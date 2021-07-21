package eu.profinit.manta.graphplayground.repository.merger.common;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import eu.profinit.manta.graphplayground.repository.merger.connector.GraphOperation;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.MantaRelationshipType;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.AttributeFormat;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.EdgeFormat;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.ItemTypes;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.LayerFormat;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.NodeFormat;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.ResourceFormat;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.SourceCodeFormat;
import eu.profinit.manta.graphplayground.model.manta.core.EdgeIdentification;
import eu.profinit.manta.graphplayground.model.manta.merger.AbstractProcessorContext;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult.ResultType;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.repository.merger.connector.SourceRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.connector.SuperRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.processor.MergerProcessor;
import eu.profinit.manta.graphplayground.repository.merger.revision.RevisionRootHandler;

import static eu.profinit.manta.graphplayground.repository.merger.common.CommonMergerTestObjects.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests for standard merger.
 *
 * @author tpolacok
 */
public abstract class CommonMergerProcessorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonMergerProcessorTest.class);

    /**
     * Driver to the Neo4j.
     */
    protected static Driver driver;

    /**
     * Neo4j service.
     */
    protected static Neo4j neo4j;

    /**
     * Standard merger processor.
     * Used for initial graph insertion - pre-test.
     */
    protected static MergerProcessor initialProcessor;

    /**
     * Merger processor.
     * Used for merging testing.
     */
    protected static MergerProcessor testedProcessor;

    /**
     * Standard processor context.
     */
    protected static AbstractProcessorContext context;

    /**
     * Utility methods for testing.
     */
    protected static TestGraphUtil testGraphUtil;

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

        beforeEachInit(mergedRevision, latestCommittedRevision);
    }

    protected abstract void beforeEachInit(Double mergedRevision, Double latestCommittedRevision);

    @Test
    public void testProcessSourceCodeNew() {
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_SOURCE_CODE);

        assertThat("Result has 1 element", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("Result is of source code type", mergerResult.getItemType(), is(ItemTypes.SOURCE_CODE));
        assertThat("Source code is new", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));

        assertThat("Context has 1 source code",
                context.getMapSourceCodeIdToDbId().keySet(), hasSize(1));
        assertThat("Context contains the merged source code",
                context.getMapSourceCodeIdToDbId().keySet(), hasItem(MERGED_SOURCE_CODE[SourceCodeFormat.ID.getIndex()]));
        assertThat("Should have 1 source nodes",
                testGraphUtil.countNodesByLabel(MantaNodeLabel.SOURCE_NODE), is(1));

        Map<String, Object> sourceCodeNodeWithControlEdge = testGraphUtil.getSourceCodeById(
                context.getMapSourceCodeIdToDbId().get(MERGED_SOURCE_CODE[SourceCodeFormat.ID.getIndex()])
        );

        testGraphUtil.compareSourceCode(MERGED_SOURCE_CODE, (Node)sourceCodeNodeWithControlEdge.get("node"));
        testGraphUtil.compareRelationshipRevision((Relationship)sourceCodeNodeWithControlEdge.get("rel"),
                REVISION_INTERVAL);

    }

    @Test
    public void testProcessSourceCodeAlreadyExistingSameRevision() {
        testGraphUtil.merge(initialProcessor, context, MERGED_SOURCE_CODE);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_SOURCE_CODE);

        assertThat("Result has 1 element", result.size() == 1);
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("Result is of source code type", mergerResult.getItemType(), is(ItemTypes.SOURCE_CODE));
        assertThat("Source code already existed", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));

        assertThat("Context has 1 source code as the duplicate is not inserted",
                context.getMapSourceCodeIdToDbId().keySet(), hasSize(1));
        assertThat("Should have 1 source nodes as duplicate is not inserted",
                testGraphUtil.countNodesByLabel(MantaNodeLabel.SOURCE_NODE), is(1));

        Map<String, Object> sourceCodeNodeWithControlEdge = testGraphUtil.getSourceCodeById(
                context.getMapSourceCodeIdToDbId().get(MERGED_SOURCE_CODE[SourceCodeFormat.ID.getIndex()])
        );

        testGraphUtil.compareSourceCode(MERGED_SOURCE_CODE, (Node)sourceCodeNodeWithControlEdge.get("node"));
        testGraphUtil.compareRelationshipRevision((Relationship)sourceCodeNodeWithControlEdge.get("rel"),
                REVISION_INTERVAL);
    }

    @Test
    public void testProcessSourceCodeAlreadyExistingUpdateRevision() {
        testGraphUtil.merge(initialProcessor, context, MERGED_SOURCE_CODE);

        context.setRevision(2.0);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_SOURCE_CODE);

        assertThat("Result has 1 element", result.size() == 1);
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("Result is of source code type", mergerResult.getItemType(), is(ItemTypes.SOURCE_CODE));
        assertThat("Source code already existed", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));

        assertThat("Context has 1 source code as the duplicate is not inserted",
                context.getMapSourceCodeIdToDbId().keySet(), hasSize(1));
        assertThat("Should have 1 source nodes as duplicate is not inserted",
                testGraphUtil.countNodesByLabel(MantaNodeLabel.SOURCE_NODE), is(1));

        Map<String, Object> sourceCodeNodeWithControlEdge = testGraphUtil.getSourceCodeById(
                context.getMapSourceCodeIdToDbId().get(MERGED_SOURCE_CODE[SourceCodeFormat.ID.getIndex()])
        );

        testGraphUtil.compareSourceCode(MERGED_SOURCE_CODE, (Node)sourceCodeNodeWithControlEdge.get("node"));
        testGraphUtil.compareRelationshipRevision((Relationship)sourceCodeNodeWithControlEdge.get("rel"),
                REVISION_INTERVAL_UPDATED);
    }

    @Test
    public void testProcessLayer() {
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_LAYER);

        assertThat("Result has 0 element", result, hasSize(0));
        assertThat("Layer should be added to context", context.getMapLayerIdToLayer().keySet(),
                hasItem(MERGED_LAYER[LayerFormat.ID.getIndex()]));
        assertThat("Layer properties should be added to context", Arrays.equals(MERGED_LAYER, MERGED_LAYER));
        assertThat("No layers inserted into database",
                testGraphUtil.countNodesByLabel(MantaNodeLabel.LAYER), is(0));
    }

    @Test
    public void testProcessResourceMissingLayer() {
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_RESOURCE);
        assertThat("Result has 2 elements", result, hasSize(2));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of layer type", mergerResult.getItemType(), is(ItemTypes.LAYER));
        assertThat("For first result, error is returned", mergerResult.getResultType(), is(ResultType.ERROR));
        mergerResult = result.get(1);
        assertThat("Second result is of resource type", mergerResult.getItemType(), is(ItemTypes.RESOURCE));
        assertThat("For second result, error is returned", mergerResult.getResultType(), is(ResultType.ERROR));
    }

    @Test
    public void testProcessResourceNewWithLayer() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_RESOURCE);

        assertThat("Result has 2 elements", result.size() == 2);
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of layer type", mergerResult.getItemType(), is(ItemTypes.LAYER));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));
        mergerResult = result.get(1);
        assertThat("Second result is of resource type", mergerResult.getItemType(), is(ItemTypes.RESOURCE));
        assertThat("For second result, new object is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));

        assertThat("Context resource map has 1 resource",
                context.getMapResourceIdToDbId().keySet(), hasSize(1));
        assertThat("Context resource map contains the merged resource",
                context.getMapResourceIdToDbId().keySet(), hasItem(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]));
        assertThat("Should have 1 resource nodes",
                testGraphUtil.countNodesByLabel(MantaNodeLabel.RESOURCE), is(1));
        assertThat("Should have 1 layer nodes",
                testGraphUtil.countNodesByLabel(MantaNodeLabel.LAYER), is(1));

        Long resourceDbId = context.getMapResourceIdToDbId().get(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]);
        assertThat("Context node-resource map has 1 element",
                context.getMapNodeDbIdResourceDbId().keySet(), hasSize(1));
        assertThat("Context node-resource maps resource to itself",
                context.getMapNodeDbIdResourceDbId().get(resourceDbId), is(resourceDbId));

        Map<String, Object> resourceLayerResult = testGraphUtil.getResourceAndLayerByResourceId(resourceDbId);
        testGraphUtil.compareResource(MERGED_RESOURCE, (Node)resourceLayerResult.get("res"));
        testGraphUtil.compareRelationshipRevision((Relationship)resourceLayerResult.get("relRes"),
                REVISION_INTERVAL);
        testGraphUtil.compareLayer(MERGED_LAYER, (Node)resourceLayerResult.get("layer"));
        testGraphUtil.compareRelationshipRevision((Relationship)resourceLayerResult.get("relLayer"),
                REVISION_INTERVAL);

    }

    @Test
    public void testProcessResourceAlreadyExistingWithLayerSameRevision() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_RESOURCE);
        assertThat("Result has 2 elements", result,  hasSize(2));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of layer type", mergerResult.getItemType(), is(ItemTypes.LAYER));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));
        mergerResult = result.get(1);
        assertThat("Second result is of resource type", mergerResult.getItemType(), is(ItemTypes.RESOURCE));
        assertThat("For second result, new object is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));

        assertThat("Context resource map has 1 resource",
                context.getMapResourceIdToDbId().keySet(), hasSize(1));
        assertThat("Context node-resource map has 1 element",
                context.getMapNodeDbIdResourceDbId().keySet(), hasSize(1));
        assertThat("Should have 1 resource nodes",
                testGraphUtil.countNodesByLabel(MantaNodeLabel.RESOURCE), is(1));
        assertThat("Should have 1 layer nodes",
                testGraphUtil.countNodesByLabel(MantaNodeLabel.LAYER), is(1));

        Long resourceDbId = context.getMapResourceIdToDbId().get(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]);
        assertThat("Context node existed set has 1 element",
                context.getNodesExistedBefore(), hasSize(1));
        assertThat("Context node existed set has the resourceId",
                context.getNodesExistedBefore(), hasItem(resourceDbId));

        Map<String, Object> resourceLayerResult = testGraphUtil.getResourceAndLayerByResourceId(resourceDbId);
        testGraphUtil.compareResource(MERGED_RESOURCE, (Node)resourceLayerResult.get("res"));
        testGraphUtil.compareRelationshipRevision((Relationship)resourceLayerResult.get("relRes"),
                REVISION_INTERVAL);
        testGraphUtil.compareLayer(MERGED_LAYER, (Node)resourceLayerResult.get("layer"));
        testGraphUtil.compareRelationshipRevision((Relationship)resourceLayerResult.get("relLayer"),
                REVISION_INTERVAL);
    }

    @Test
    public void testProcessResourceAlreadyExistingWithLayerUpdateRevision() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);

        context.setRevision(2.0);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_RESOURCE);

        assertThat("Result has 2 elements", result,  hasSize(2));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of layer type", mergerResult.getItemType(), is(ItemTypes.LAYER));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));
        mergerResult = result.get(1);
        assertThat("Second result is of resource type", mergerResult.getItemType(), is(ItemTypes.RESOURCE));
        assertThat("For second result, new object is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));

        assertThat("Should have 1 resource nodes",
                testGraphUtil.countNodesByLabel(MantaNodeLabel.RESOURCE), is(1));
        assertThat("Should have 1 layer nodes",
                testGraphUtil.countNodesByLabel(MantaNodeLabel.LAYER), is(1));

        Long resourceDbId = context.getMapResourceIdToDbId().get(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]);
        assertThat("Context node existed set has 1 element",
                context.getNodesExistedBefore(), hasSize(1));
        assertThat("Context node existed set has the resourceId",
                context.getNodesExistedBefore(), hasItem(resourceDbId));

        Map<String, Object> resourceLayerResult = testGraphUtil.getResourceAndLayerByResourceId(resourceDbId);
        testGraphUtil.compareResource(MERGED_RESOURCE, (Node)resourceLayerResult.get("res"));
        testGraphUtil.compareRelationshipRevision((Relationship)resourceLayerResult.get("relRes"),
                REVISION_INTERVAL_UPDATED);
        testGraphUtil.compareLayer(MERGED_LAYER, (Node)resourceLayerResult.get("layer"));
        testGraphUtil.compareRelationshipRevision((Relationship)resourceLayerResult.get("relLayer"),
                REVISION_INTERVAL_UPDATED);
    }

    @Test
    public void testProcessResourceAlreadyExistingMissingLayer() {
        testGraphUtil.createBasicMergeGraph(REVISION_INTERVAL);
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_RESOURCE);

        assertThat("Result has 2 elements", result,  hasSize(2));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of layer type", mergerResult.getItemType(), is(ItemTypes.LAYER));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));
        mergerResult = result.get(1);
        assertThat("Second result is of resource type", mergerResult.getItemType(), is(ItemTypes.RESOURCE));
        assertThat("For second result, new object is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));

        Long resourceDbId = context.getMapResourceIdToDbId().get(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]);
        assertThat("Context node existed set has 1 element",
                context.getNodesExistedBefore(), hasSize(1));
        assertThat("Context node existed set has the resourceId",
                context.getNodesExistedBefore(), hasItem(resourceDbId));

        Map<String, Object> resourceLayerResult = testGraphUtil.getResourceAndLayerByResourceId(resourceDbId);
        testGraphUtil.compareLayer(MERGED_LAYER, (Node)resourceLayerResult.get("layer"));
        testGraphUtil.compareRelationshipRevision((Relationship)resourceLayerResult.get("relLayer"),
                REVISION_INTERVAL);
    }

    @Test
    public void testProcessNodeMissingResource() {
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_RESOURCE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node type", mergerResult.getItemType(), is(ItemTypes.NODE));
        assertThat("For first result, error is returned", mergerResult.getResultType(), is(ResultType.ERROR));
    }

    @Test
    public void testProcessNodeMissingParent() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_PARENT);
        assertThat("Result has 1 elements", result, hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node type", mergerResult.getItemType(), is(ItemTypes.NODE));
        assertThat("For first result, error is returned", mergerResult.getResultType(), is(ResultType.ERROR));
    }


    @Test
    public void testProcessNodeNewUnderResource() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_RESOURCE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node type", mergerResult.getItemType(), is(ItemTypes.NODE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));

        assertThat("Context node local-database map has 1 element",
                context.getMapNodeIdToDbId().keySet(), hasSize(1));
        assertThat("Context node local-database map has the new node",
                context.getMapNodeIdToDbId().containsKey(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]));

        Long dbId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]);

        assertThat("Context node-resource map has 2 elements",
                context.getMapNodeDbIdResourceDbId().keySet(), hasSize(2));
        assertThat("Context node-resource map has the nodeId",
                context.getMapNodeDbIdResourceDbId(), hasKey(dbId));

        Long resourceId = context.getMapResourceIdToDbId().get(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]);
        assertThat("Context node-resource has correct resource for new node",
                context.getMapNodeDbIdResourceDbId().get(dbId), is(resourceId));

        Map<String, Object> nodeResult = testGraphUtil.getNodeById(dbId, resourceId);
        testGraphUtil.compareNode(MERGED_NODE_RESOURCE, (Node)nodeResult.get("node"));
        testGraphUtil.compareRelationshipRevision((Relationship)nodeResult.get("rel"),
                REVISION_INTERVAL);
    }

    @Test
    public void testProcessNodeNewUnderNodeResourceInherited() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_PARENT);
        assertThat("Result has 1 elements", result, hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node type", mergerResult.getItemType(), is(ItemTypes.NODE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));

        assertThat("Context node local-database map has 2 elements",
                context.getMapNodeIdToDbId().keySet(), hasSize(2));
        assertThat("Context node local-database map has the new node",
                context.getMapNodeIdToDbId(), hasKey(MERGED_NODE_PARENT[NodeFormat.ID.getIndex()]));

        Long dbId = context.getMapNodeIdToDbId().get(MERGED_NODE_PARENT[NodeFormat.ID.getIndex()]);
        assertThat("Context node-resource map has 3 elements",
                context.getMapNodeDbIdResourceDbId().keySet(), hasSize(3));
        assertThat("Context node-resource map has the nodeId",
                context.getMapNodeDbIdResourceDbId(), hasKey(dbId));

        Long resourceId = context.getMapResourceIdToDbId().get(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]);
        assertThat("Context node-resource has correct inherited resource for new node",
                context.getMapNodeDbIdResourceDbId().get(dbId), is(resourceId));

        Long parentId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]);

        Map<String, Object> nodeResult = testGraphUtil.getNodeById(dbId, parentId);
        testGraphUtil.compareNode(MERGED_NODE_PARENT, (Node)nodeResult.get("node"));
        testGraphUtil.compareRelationshipRevision((Relationship)nodeResult.get("rel"), REVISION_INTERVAL);
    }

    @Test
    public void testProcessNodeNewResourceAndParentDifferent() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_PARENT_DIFFERENT_RESOURCE);
        assertThat("Result has 1 elements", result, hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node type", mergerResult.getItemType(), is(ItemTypes.NODE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));

        assertThat("Context node local-database map has 2 elements",
                context.getMapNodeIdToDbId().keySet(), hasSize(2));
        assertThat("Context node local-database map has the new node",
                context.getMapNodeIdToDbId().containsKey(MERGED_NODE_PARENT[NodeFormat.ID.getIndex()]));

        Long dbId = context.getMapNodeIdToDbId().get(MERGED_NODE_PARENT[NodeFormat.ID.getIndex()]);
        assertThat("Context node-resource map has 4 elements",
                context.getMapNodeDbIdResourceDbId().keySet(), hasSize(4));
        assertThat("Context node-resource map has the nodeId",
                context.getMapNodeDbIdResourceDbId(), hasKey(dbId));

        Long resourceId = context.getMapResourceIdToDbId().get(MERGED_RESOURCE_2[ResourceFormat.ID.getIndex()]);
        assertThat("Context node-resource has correct resource for new node",
                context.getMapNodeDbIdResourceDbId().get(dbId), is(resourceId));

        Long parentId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]);

        Map<String, Object> nodeResult = testGraphUtil.getNodeByIdParentResource(dbId, parentId, resourceId);
        testGraphUtil.compareNode(MERGED_NODE_PARENT_DIFFERENT_RESOURCE, (Node)nodeResult.get("node"));
        testGraphUtil.compareRelationshipRevision((Relationship)nodeResult.get("relParent"),
                REVISION_INTERVAL);
        testGraphUtil.compareRelationshipRevision((Relationship)nodeResult.get("relRes"),
                REVISION_INTERVAL);
    }



    @Test
    public void testProcessNodeAlreadyExistingSameRevision() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);

        // add this as one input request should not contain duplicates
        context.getNodesExistedBefore().add(context.getMapResourceIdToDbId().get(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]));

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_RESOURCE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node type", mergerResult.getItemType(), is(ItemTypes.NODE));
        assertThat("For first result, already exist is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));

        assertThat("Context node local-database map has 1 element",
                context.getMapNodeIdToDbId().keySet(), hasSize(1));
        assertThat("Context node local-database map has the new node",
                context.getMapNodeIdToDbId().containsKey(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]));

        Long dbId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]);
        assertThat("Context node-resource map has 2 elements",
                context.getMapNodeDbIdResourceDbId().keySet(), hasSize(2));
        assertThat("Context node-resource map has the nodeId",
                context.getMapNodeDbIdResourceDbId(), hasKey(dbId));

        Long resourceId = context.getMapResourceIdToDbId().get(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]);
        assertThat("Context node-resource has correct resource for new node",
                context.getMapNodeDbIdResourceDbId().get(dbId), is(resourceId));

        assertThat("Context node existed set has 2 elements",
                context.getNodesExistedBefore(), hasSize(2));
        assertThat("Context node existed set has the nodeId",
                context.getNodesExistedBefore(), hasItem(dbId));

        Map<String, Object> nodeResult = testGraphUtil.getNodeById(dbId, resourceId);
        testGraphUtil.compareNode(MERGED_NODE_RESOURCE, (Node)nodeResult.get("node"));
        testGraphUtil.compareRelationshipRevision((Relationship)nodeResult.get("rel"), REVISION_INTERVAL);
    }

    @Test
    public void testProcessNodeAlreadyExistingUpdateRevision() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);

        // add this as one input request should not contain duplicates
        context.getNodesExistedBefore().add(context.getMapResourceIdToDbId().get(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]));
        context.setRevision(2.0);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_RESOURCE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node type", mergerResult.getItemType(), is(ItemTypes.NODE));
        assertThat("For first result, already exist is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));

        assertThat("Context node local-database map has 1 element",
                context.getMapNodeIdToDbId().keySet(), hasSize(1));
        assertThat("Context node local-database map has the new node",
                context.getMapNodeIdToDbId().containsKey(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]));

        Long dbId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]);
        assertThat("Context node-resource map has 2 elements",
                context.getMapNodeDbIdResourceDbId().keySet(), hasSize(2));
        assertThat("Context node-resource map has the nodeId",
                context.getMapNodeDbIdResourceDbId(), hasKey(dbId));

        Long resourceId = context.getMapResourceIdToDbId().get(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]);
        assertThat("Context node-resource has correct resource for new node",
                context.getMapNodeDbIdResourceDbId().get(dbId), is(resourceId));
        assertThat("Context node existed set has 2 elements",
                context.getNodesExistedBefore(), hasSize(2));
        assertThat("Context node existed set has the nodeId",
                context.getNodesExistedBefore(), hasItem(dbId));

        Map<String, Object> nodeResult = testGraphUtil.getNodeById(dbId, resourceId);
        testGraphUtil.compareNode(MERGED_NODE_RESOURCE, (Node)nodeResult.get("node"));
        testGraphUtil.compareRelationshipRevision((Relationship)nodeResult.get("rel"), REVISION_INTERVAL_UPDATED);
    }

    @Test
    public void testProcessNodeFlagRemoveMyself() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT_DIFFERENT_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_ATTRIBUTE);
        testGraphUtil.merge(initialProcessor, context, MERGED_PERSPECTIVE_EDGE);


        int initialSize = testGraphUtil.getEdgesWithSpecificRevision(REVISION_INTERVAL).size();
        assertThat("Number of edges belonging into <1.0,1.999999> revision",
                initialSize,
                is(13));

        context.getNodesExistedBefore().add(
                context.getMapResourceIdToDbId().get(MERGED_RESOURCE[ResourceFormat.ID.getIndex()])
        );
        context.setLatestCommittedRevision(1.0);
        context.setRevision(2.0);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_REMOVE_MYSELF);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node type", mergerResult.getItemType(), is(ItemTypes.NODE));
        assertThat("For first result, already exist is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));

        Long nodeId = context.getMapNodeIdToDbId().get(MERGED_NODE_REMOVE_MYSELF[NodeFormat.ID.getIndex()]);
        Long resourceId = context.getMapResourceIdToDbId().get(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]);
        Map<String, Object> parentEdgeResult = testGraphUtil.getRelationship(nodeId, resourceId, MantaRelationshipType.HAS_RESOURCE);

        Relationship controlEdge = (Relationship)parentEdgeResult.get("rel");
        List<Relationship> unchangedEdges = testGraphUtil.getEdgesWithSpecificRevision(REVISION_INTERVAL);
        List<Relationship> edges = testGraphUtil.getSubtreeEdges(nodeId).stream()
                .filter(e -> e.id() != controlEdge.id()).collect(Collectors.toList());

        assertThat("Number of edges belonging into <1.0,1.0> revision",
                edges.size(),
                is(7));

        assertThat("Number of edges belonging into <1.0,1.999999> revision",
                unchangedEdges.size(),
                is(initialSize - edges.size() - 1));

        testGraphUtil.compareRelationshipRevision((Relationship)parentEdgeResult.get("rel") , REVISION_INTERVAL_CLOSED);

        edges.forEach(edge -> testGraphUtil.compareRelationshipRevision(edge, REVISION_INTERVAL_CLOSED));
    }

    @Test
    public void testProcessNodeFlagRemove() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT_DIFFERENT_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_ATTRIBUTE);
        testGraphUtil.merge(initialProcessor, context, MERGED_PERSPECTIVE_EDGE);


        int initialSize = testGraphUtil.getEdgesWithSpecificRevision(REVISION_INTERVAL).size();
        assertThat("Number of edges belonging into <1.0,1.999999> revision",
                initialSize,
                is(13));

        context.getNodesExistedBefore().add(
                context.getMapResourceIdToDbId().get(MERGED_RESOURCE[ResourceFormat.ID.getIndex()])
        );
        context.setLatestCommittedRevision(1.0);
        context.setRevision(2.0);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_REMOVE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node type", mergerResult.getItemType(), is(ItemTypes.NODE));
        assertThat("For first result, already exist is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));

        Long nodeId = context.getMapNodeIdToDbId().get(MERGED_NODE_REMOVE[NodeFormat.ID.getIndex()]);
        Long resourceId = context.getMapResourceIdToDbId().get(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]);
        Map<String, Object> parentEdgeResult = testGraphUtil.getRelationship(nodeId, resourceId, MantaRelationshipType.HAS_RESOURCE);
        Relationship controlEdge = (Relationship)parentEdgeResult.get("rel");
        List<Relationship> unchangedEdges = testGraphUtil.getEdgesWithSpecificRevision(REVISION_INTERVAL);
        List<Relationship> edges = testGraphUtil.getSubtreeEdges(nodeId).stream()
                .filter(e -> e.id() != controlEdge.id()).collect(Collectors.toList());

        assertThat("Number of edges belonging into <1.0,1.0> revision",
                edges.size(),
                is(7));

        assertThat("Number of edges belonging into <1.0,1.999999> revision",
                unchangedEdges.size(),
                is(initialSize - edges.size() - 1));


        testGraphUtil.compareRelationshipRevision(controlEdge , REVISION_INTERVAL_UPDATED);

        edges.forEach(edge -> testGraphUtil.compareRelationshipRevision(edge, REVISION_INTERVAL_CLOSED));
    }

    @Test
    public void testProcessNodeAttributeMissingNode() {
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_ATTRIBUTE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node attribute type", mergerResult.getItemType(), is(ItemTypes.NODE_ATTRIBUTE));
        assertThat("For first result, error is returned", mergerResult.getResultType(), is(ResultType.ERROR));
    }

    @Test
    public void testProcessNodeAttributeNew() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_ATTRIBUTE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node attribute type", mergerResult.getItemType(), is(ItemTypes.NODE_ATTRIBUTE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));

        Long nodeId = context.getMapNodeIdToDbId().get(MERGED_NODE_ATTRIBUTE[AttributeFormat.OBJECT_ID.getIndex()]);

        Map<String, Object> nodeResult = testGraphUtil.getNodeAttribute(nodeId, MERGED_NODE_ATTRIBUTE[2], MERGED_NODE_ATTRIBUTE[3]);
        testGraphUtil.compareNodeAttribute(MERGED_NODE_ATTRIBUTE, (Node)nodeResult.get("attr"));
        testGraphUtil.compareRelationshipRevision((Relationship)nodeResult.get("rel"), REVISION_INTERVAL);
    }

    @Test
    public void testProcessNodeAttributeAlreadyExistingSameRevision() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_ATTRIBUTE);

        // add this as one input request should not contain duplicates
        context.getNodesExistedBefore().add(context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]));

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_ATTRIBUTE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node attribute type", mergerResult.getItemType(), is(ItemTypes.NODE_ATTRIBUTE));
        assertThat("For first result, already exist is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));

        Long nodeId = context.getMapNodeIdToDbId().get(MERGED_NODE_ATTRIBUTE[AttributeFormat.OBJECT_ID.getIndex()]);

        Map<String, Object> nodeResult = testGraphUtil.getNodeAttribute(nodeId, MERGED_NODE_ATTRIBUTE[2], MERGED_NODE_ATTRIBUTE[3]);
        testGraphUtil.compareNodeAttribute(MERGED_NODE_ATTRIBUTE, (Node)nodeResult.get("attr"));
        testGraphUtil.compareRelationshipRevision((Relationship)nodeResult.get("rel"), REVISION_INTERVAL);
    }

    @Test
    public void testProcessNodeAttributeAlreadyExistingUpdateRevision() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_ATTRIBUTE);

        // add this as one input request should not contain duplicates
        context.getNodesExistedBefore().add(context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]));
        context.setRevision(2.0);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_ATTRIBUTE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node attribute type", mergerResult.getItemType(), is(ItemTypes.NODE_ATTRIBUTE));
        assertThat("For first result, already exist is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));

        Long nodeId = context.getMapNodeIdToDbId().get(MERGED_NODE_ATTRIBUTE[AttributeFormat.OBJECT_ID.getIndex()]);

        Map<String, Object> nodeResult = testGraphUtil.getNodeAttribute(nodeId, MERGED_NODE_ATTRIBUTE[2], MERGED_NODE_ATTRIBUTE[3]);
        testGraphUtil.compareNodeAttribute(MERGED_NODE_ATTRIBUTE, (Node)nodeResult.get("attr"));
        testGraphUtil.compareRelationshipRevision((Relationship)nodeResult.get("rel"), REVISION_INTERVAL_UPDATED);
    }

    @Test
    public void testProcessNodeAttributeSourceCodeMissing() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_ATTRIBUTE_SOURCE_CODE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node attribute type", mergerResult.getItemType(), is(ItemTypes.NODE_ATTRIBUTE));
        assertThat("For first result, error is returned", mergerResult.getResultType(), is(ResultType.ERROR));
    }


    @Test
    public void testProcessNodeAttributeSourceCodeNew() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_SOURCE_CODE);
        System.out.println("ALO " + List.of(MERGED_NODE_ATTRIBUTE_SOURCE_CODE));
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_ATTRIBUTE_SOURCE_CODE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node attribute type", mergerResult.getItemType(), is(ItemTypes.NODE_ATTRIBUTE));
        assertThat("For first result, error is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));

        Long nodeId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]);
        String sourceCodeId = context.getMapSourceCodeIdToDbId().get(MERGED_SOURCE_CODE[SourceCodeFormat.ID.getIndex()]);
        Map<String, Object> nodeResult = testGraphUtil.getNodeAttribute(nodeId,
                MERGED_NODE_ATTRIBUTE_SOURCE_CODE[AttributeFormat.KEY.getIndex()], sourceCodeId);

        String[] expected = Arrays.copyOf(MERGED_NODE_ATTRIBUTE_SOURCE_CODE, MERGED_NODE_ATTRIBUTE_SOURCE_CODE.length);
        expected[AttributeFormat.VALUE.getIndex()] = sourceCodeId;

        testGraphUtil.compareNodeAttribute(expected, (Node)nodeResult.get("attr"));
        testGraphUtil.compareRelationshipRevision((Relationship)nodeResult.get("rel"), REVISION_INTERVAL);
    }

    @Test
    public void testProcessNodeAttributeMapsToNew() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_ATTRIBUTE_MAPS_TO);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge type", mergerResult.getItemType(), is(ItemTypes.EDGE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));

        Long sourceId = context.getMapNodeIdToDbId().get(MERGED_NODE_PARENT[NodeFormat.ID.getIndex()]);
        Long targetId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]);

        Map<String, Object> nodeResult = testGraphUtil.getRelationship(sourceId, targetId, MantaRelationshipType.MAPS_TO);
        testGraphUtil.compareRelationshipRevision((Relationship)nodeResult.get("rel"), REVISION_INTERVAL);
    }

    @Test
    public void testProcessNodeAttributeMapsToNewMissingMappedNode() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_ATTRIBUTE_MAPS_TO_MISSING);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge type", mergerResult.getItemType(), is(ItemTypes.EDGE));
        assertThat("For first result, error is returned", mergerResult.getResultType(), is(ResultType.ERROR));
    }

    @Test
    public void testProcessNodeAttributeMapsToAlreadyExistingSameRevision() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_ATTRIBUTE_MAPS_TO);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_ATTRIBUTE_MAPS_TO);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge type", mergerResult.getItemType(), is(ItemTypes.EDGE));
        assertThat("For first result, already exist is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));

        Long sourceId = context.getMapNodeIdToDbId().get(MERGED_NODE_PARENT[NodeFormat.ID.getIndex()]);
        Long targetId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]);

        Map<String, Object> nodeResult = testGraphUtil.getRelationship(sourceId, targetId, MantaRelationshipType.MAPS_TO);
        testGraphUtil.compareRelationshipRevision((Relationship)nodeResult.get("rel"), REVISION_INTERVAL);
    }

    @Test
    public void testProcessNodeAttributeMapsToAlreadyExistingUpdateRevision() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_ATTRIBUTE_MAPS_TO);

        context.setRevision(2.0);
        context.setLatestCommittedRevision(1.0);
        // merge resource and nodes into new revision - required for mapsTo edges
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_ATTRIBUTE_MAPS_TO);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge type", mergerResult.getItemType(), is(ItemTypes.EDGE));
        assertThat("For first result, alraedy exist is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));

        Long sourceId = context.getMapNodeIdToDbId().get(MERGED_NODE_PARENT[NodeFormat.ID.getIndex()]);
        Long targetId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]);

        Map<String, Object> nodeResult = testGraphUtil.getRelationship(sourceId, targetId, MantaRelationshipType.MAPS_TO);
        testGraphUtil.compareRelationshipRevision((Relationship)nodeResult.get("rel"), REVISION_INTERVAL_UPDATED);
    }

    @Test
    public void testProcessEdgeNewMissingNode() {
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_EDGE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge type", mergerResult.getItemType(), is(ItemTypes.EDGE));
        assertThat("For first result, error is returned", mergerResult.getResultType(), is(ResultType.ERROR));
    }

    @Test
    public void testProcessEdgeNew() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_EDGE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge type", mergerResult.getItemType(), is(ItemTypes.EDGE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));

        assertThat("Context edges map has 1 element",
                context.getMapEdgeIdToDbId().keySet(), hasSize(1));
        assertThat("Context edges map has inserted edge",
                context.getMapEdgeIdToDbId(), hasKey(MERGED_EDGE[EdgeFormat.ID.getIndex()]));

        Map<String, Object> edgeResult = testGraphUtil.getRelationship(context.getMapEdgeIdToDbId().get(MERGED_EDGE[EdgeFormat.ID.getIndex()]));

        testGraphUtil.compareRelationship((Relationship)edgeResult.get("rel"), REVISION_INTERVAL,
                MantaRelationshipType.DIRECT, Map.of());
    }

    @Test
    public void testProcessEdgeAlreadyExistingSameRevision() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);
        testGraphUtil.merge(initialProcessor, context, MERGED_EDGE);

        // add this as one input request should not contain duplicates
        context.getNodesExistedBefore().add(context.getMapNodeIdToDbId().get(MERGED_EDGE[EdgeFormat.SOURCE.getIndex()]));
        context.getNodesExistedBefore().add(context.getMapNodeIdToDbId().get(MERGED_EDGE[EdgeFormat.TARGET.getIndex()]));

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_EDGE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge type", mergerResult.getItemType(), is(ItemTypes.EDGE));
        assertThat("For first result, already exist is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));

        assertThat("Context edges map has 1 element",
                context.getMapEdgeIdToDbId().keySet(), hasSize(1));
        assertThat("Context edges map has inserted edge",
                context.getMapEdgeIdToDbId(), hasKey(MERGED_EDGE[EdgeFormat.ID.getIndex()]));

        Map<String, Object> edgeResult = testGraphUtil.getRelationship(context.getMapEdgeIdToDbId().get(MERGED_EDGE[EdgeFormat.ID.getIndex()]));

        testGraphUtil.compareRelationship((Relationship)edgeResult.get("rel"), REVISION_INTERVAL,
                MantaRelationshipType.DIRECT, Map.of());
    }

    @Test
    public void testProcessEdgeAlreadyExistingUpdateRevision() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);
        testGraphUtil.merge(initialProcessor, context, MERGED_EDGE);

        // add this as one input request should not contain duplicates
        context.getNodesExistedBefore().add(context.getMapNodeIdToDbId().get(MERGED_EDGE[EdgeFormat.SOURCE.getIndex()]));
        context.getNodesExistedBefore().add(context.getMapNodeIdToDbId().get(MERGED_EDGE[EdgeFormat.TARGET.getIndex()]));
        context.setRevision(2.0);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_EDGE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge type", mergerResult.getItemType(), is(ItemTypes.EDGE));
        assertThat("For first result, already exist is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));

        assertThat("Context edges map has 1 element",  context.getMapEdgeIdToDbId().keySet(), hasSize(1));
        assertThat("Context edges map has inserted edge", context.getMapEdgeIdToDbId(), hasKey(MERGED_EDGE[EdgeFormat.ID.getIndex()]));

        Map<String, Object> edgeResult = testGraphUtil.getRelationship(context.getMapEdgeIdToDbId().get(MERGED_EDGE[EdgeFormat.ID.getIndex()]));
        testGraphUtil.compareRelationship((Relationship)edgeResult.get("rel"), REVISION_INTERVAL_UPDATED,
                MantaRelationshipType.DIRECT, Map.of());
    }

    @Test
    public void testProcessEdgePerspectiveNew() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);

        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE_2);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_PERSPECTIVE_EDGE);
        assertThat("Result has 3 elements", result,  hasSize(3));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node attribute type", mergerResult.getItemType(), is(ItemTypes.NODE_ATTRIBUTE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));
        mergerResult = result.get(1);
        assertThat("First result is of edge attribute type", mergerResult.getItemType(), is(ItemTypes.EDGE_ATTRIBUTE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));
        mergerResult = result.get(2);
        assertThat("First result is of edge type", mergerResult.getItemType(), is(ItemTypes.EDGE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));

        Long sourceId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]);
        Long targetId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE_2[NodeFormat.ID.getIndex()]);

        Map<String, Object> edgeResult = testGraphUtil.getRelationship(sourceId, targetId, MantaRelationshipType.PERSPECTIVE);
        Map<String, Object> nodeAttributeResult = testGraphUtil.getAllNodeAttributes(sourceId);

        testGraphUtil.compareNodeAttribute(
                new String[]{"", "", "Logical perspective", "Oracle DDL Second[Oracle scripts Second].manta_logical[Connection_logical]"},
                (Node)nodeAttributeResult.get("attr"));
        testGraphUtil.compareRelationshipRevision((Relationship)nodeAttributeResult.get("rel"), REVISION_INTERVAL);
        testGraphUtil.compareRelationship((Relationship)edgeResult.get("rel"), REVISION_INTERVAL,
                MantaRelationshipType.PERSPECTIVE, Map.of("layer", "Logical"));
    }

    @Test
    public void testProcessEdgePerspectiveNewInvalid() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);

        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE_2);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_PERSPECTIVE_EDGE_INVALID);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge type", mergerResult.getItemType(), is(ItemTypes.EDGE));
        assertThat("For first result, error is returned", mergerResult.getResultType(), is(ResultType.ERROR));
    }

    @Test
    public void testProcessEdgeAttributeMissingEdge() {
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_EDGE_ATTRIBUTE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge attribute type", mergerResult.getItemType(), is(ItemTypes.EDGE_ATTRIBUTE));
        assertThat("For first result, error is returned", mergerResult.getResultType(), is(ResultType.ERROR));
    }

    @Test
    public void testProcessEdgeAttributeAddNewProperty() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);
        testGraphUtil.merge(initialProcessor, context, MERGED_EDGE);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_EDGE_ATTRIBUTE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge attribute type", mergerResult.getItemType(), is(ItemTypes.EDGE_ATTRIBUTE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));

        Map<String, Object> edgeResult = testGraphUtil.getRelationship(context.getMapEdgeIdToDbId().get(MERGED_EDGE[EdgeFormat.ID.getIndex()]));
        testGraphUtil.compareRelationship((Relationship)edgeResult.get("rel"), REVISION_INTERVAL,
                MantaRelationshipType.DIRECT, Map.of(
                        MERGED_EDGE_ATTRIBUTE[AttributeFormat.KEY.getIndex()],
                        MERGED_EDGE_ATTRIBUTE[AttributeFormat.VALUE.getIndex()]));
    }

    @Test
    public void testProcessEdgeAttributeSamePropertySameValue() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);
        testGraphUtil.merge(initialProcessor, context, MERGED_EDGE);
        testGraphUtil.merge(initialProcessor, context, MERGED_EDGE_ATTRIBUTE);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_EDGE_ATTRIBUTE);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge attribute type", mergerResult.getItemType(), is(ItemTypes.EDGE_ATTRIBUTE));
        assertThat("For first result, already exist is returned", mergerResult.getResultType(), is(ResultType.ALREADY_EXIST));
    }

    @Test
    public void testProcessEdgeAttributeSamePropertyDifferentValue() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);
        testGraphUtil.merge(initialProcessor, context, MERGED_EDGE);
        testGraphUtil.merge(initialProcessor, context, MERGED_EDGE_ATTRIBUTE);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_EDGE_ATTRIBUTE_2);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge attribute type", mergerResult.getItemType(), is(ItemTypes.EDGE_ATTRIBUTE));
        assertThat("For first result, already exist is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));

        Map<String, Object> edgeResult = testGraphUtil.getRelationship(context.getMapEdgeIdToDbId().get(MERGED_EDGE[EdgeFormat.ID.getIndex()]));
        testGraphUtil.compareRelationship((Relationship)edgeResult.get("rel"), REVISION_INTERVAL,
                MantaRelationshipType.DIRECT, Map.of(
                        MERGED_EDGE_ATTRIBUTE_2[AttributeFormat.KEY.getIndex()],
                        MERGED_EDGE_ATTRIBUTE_2[AttributeFormat.VALUE.getIndex()]));
    }

    @Test
    public void testProcessEdgeAttributeCopyEdge() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);
        testGraphUtil.merge(initialProcessor, context, MERGED_EDGE);
        testGraphUtil.merge(initialProcessor, context, MERGED_EDGE_ATTRIBUTE);

        EdgeIdentification oldEdge = context.getEdgeDbId(MERGED_EDGE[EdgeFormat.ID.getIndex()]);

        // Changed latest committed revision - required to induce this state
        context.setLatestCommittedRevision(1.0);
        context.setRevision(2.0);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_EDGE_ATTRIBUTE_2);
        assertThat("Result has 1 elements", result,  hasSize(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge attribute type", mergerResult.getItemType(), is(ItemTypes.EDGE_ATTRIBUTE));
        assertThat("For first result, already exist is returned", mergerResult.getResultType(), is(ResultType.NEW_OBJECT));

        EdgeIdentification newEdge = context.getEdgeDbId(MERGED_EDGE[EdgeFormat.ID.getIndex()]);
        assertThat("Edge was replaced", oldEdge.getRelationId(), not(newEdge.getRelationId()));

        Map<String, Object> edgeResult = testGraphUtil.getRelationship(oldEdge);
        testGraphUtil.compareRelationship((Relationship)edgeResult.get("rel"), new RevisionInterval(1.0),
                MantaRelationshipType.DIRECT, Map.of(
                        MERGED_EDGE_ATTRIBUTE[AttributeFormat.KEY.getIndex()],
                        MERGED_EDGE_ATTRIBUTE[AttributeFormat.VALUE.getIndex()]));

        edgeResult = testGraphUtil.getRelationship(newEdge);
        testGraphUtil.compareRelationship((Relationship)edgeResult.get("rel"), new RevisionInterval(2.0, 2.999999),
                MantaRelationshipType.DIRECT, Map.of(
                        MERGED_EDGE_ATTRIBUTE_2[AttributeFormat.KEY.getIndex()],
                        MERGED_EDGE_ATTRIBUTE_2[AttributeFormat.VALUE.getIndex()]));
    }

}
