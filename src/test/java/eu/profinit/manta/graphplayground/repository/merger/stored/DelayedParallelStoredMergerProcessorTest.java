package eu.profinit.manta.graphplayground.repository.merger.stored;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.EdgeProperty;
import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.MantaRelationshipType;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.*;
import eu.profinit.manta.graphplayground.model.manta.core.EdgeIdentification;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.context.ParallelStoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.merger.parallel.processor.DelayedParallelStoredMergerProcessor;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedEdgeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedNodeAttributeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedPerspectiveEdgeQuery;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphCreation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphOperation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSourceRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSuperRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.types.Relationship;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static eu.profinit.manta.graphplayground.repository.merger.common.CommonMergerTestObjects.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Delayed parallel merger processor tests.
 *
 * @author tpolacok.
 */
public class DelayedParallelStoredMergerProcessorTest extends ParallelStoredMergerProcessorTest {

    @Override
    protected void beforeEachInit(Double mergedRevision, Double latestCommittedRevision) {
        testGraphUtil = new StoredTestGraphUtil(driver, neo4j.defaultDatabaseService());
        StoredRevisionUtils revisionUtils = new StoredRevisionUtils();
        StoredGraphOperation graphOperation = new StoredGraphOperation(revisionUtils);
        StoredGraphCreation graphCreation = new StoredGraphCreation(graphOperation);
        context = new ParallelStoredProcessorContext(mergedRevision, latestCommittedRevision);
        initialProcessor = new StoredMergerProcessor(
                new StoredSuperRootHandler(), new StoredSourceRootHandler(graphOperation), new StoredRevisionRootHandler(),
                graphOperation, graphCreation, revisionUtils, LOGGER
        );

        testedProcessor = new DelayedParallelStoredMergerProcessor(
                new StoredSuperRootHandler(), new StoredSourceRootHandler(graphOperation), new StoredRevisionRootHandler(),
                graphOperation, graphCreation, revisionUtils, LOGGER
        );
    }

    @Override
    @Test
    public void testProcessNodeAttributeMapsToNew() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_ATTRIBUTE_MAPS_TO);
        assertThat("Result has 1 elements", result.size(), is(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge type", mergerResult.getItemType(), is(ItemTypes.EDGE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ProcessingResult.ResultType.NEW_OBJECT));

        Long sourceId = context.getMapNodeIdToDbId().get(MERGED_NODE_PARENT[NodeFormat.ID.getIndex()]);
        Long targetId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]);

        Map<String, Object> nodeResult = testGraphUtil.getRelationship(sourceId, targetId, MantaRelationshipType.MAPS_TO);
        assertThat("Relationship was not created", nodeResult.keySet(), hasSize(0));
        assertThat("Relationship creation description was stored.", ((ParallelStoredProcessorContext)context).getEdgeQueryList(), hasSize(1));
        DelayedEdgeQuery query = (DelayedEdgeQuery)((ParallelStoredProcessorContext)context).getEdgeQueryList().get(0);
        assertThat("Query type", query.getEdgeType(), is(MantaRelationshipType.MAPS_TO));
        assertThat("Query local identifier", query.getLocalId(), is(""));
        assertThat("Query source identifier", query.getSourceNodeId(), is(sourceId));
        assertThat("Query target identifier", query.getTargetNodeId(), is(targetId));
        assertThat("Query in correct revision interval", query.getRevisionInterval(), is(REVISION_INTERVAL));
    }

    @Override
    @Test
    public void testProcessNodeAttributeNew() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_ATTRIBUTE);
        assertThat("Result has 1 elements", result.size(), is(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node attribute type", mergerResult.getItemType(), is(ItemTypes.NODE_ATTRIBUTE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ProcessingResult.ResultType.NEW_OBJECT));

        Long nodeId = context.getMapNodeIdToDbId().get(MERGED_NODE_ATTRIBUTE[AttributeFormat.OBJECT_ID.getIndex()]);

        Map<String, Object> nodeResult = testGraphUtil.getNodeAttribute(nodeId, MERGED_NODE_ATTRIBUTE[2], MERGED_NODE_ATTRIBUTE[3]);

        assertThat("Node attribute was not created", nodeResult.keySet(), hasSize(0));
        assertThat("Node attribute creation description was stored.",
                ((ParallelStoredProcessorContext)context).getNodeAttributeQueryList(), hasSize(1));
        DelayedNodeAttributeQuery query = (DelayedNodeAttributeQuery)((ParallelStoredProcessorContext)context)
                .getNodeAttributeQueryList().get(0);
        assertThat("Query key", query.getAttributeKey(), is(MERGED_NODE_ATTRIBUTE[AttributeFormat.KEY.getIndex()]));
        assertThat("Query value", query.getAttributeValue(), is(MERGED_NODE_ATTRIBUTE[AttributeFormat.VALUE.getIndex()]));
        assertThat("Query source identifier", query.getNodeId(), is(nodeId));
        assertThat("Query in correct revision interval", query.getRevisionInterval(), is(REVISION_INTERVAL));
    }

    @Override
    @Test
    public void testProcessEdgeAttributeCopyEdge() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);
        testGraphUtil.merge(initialProcessor, context, MERGED_EDGE);
        testGraphUtil.merge(initialProcessor, context, MERGED_EDGE_ATTRIBUTE);

        EdgeIdentification oldEdge = context.getEdgeDbId(MERGED_EDGE[EdgeFormat.ID.getIndex()]);

        Map<String, Object> edgeResult = testGraphUtil.getRelationship(oldEdge);
        Relationship edge = (Relationship) edgeResult.get("rel");

        // Changed latest committed revision - required to induce this state
        context.setLatestCommittedRevision(1.0);
        context.setRevision(2.0);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_EDGE_ATTRIBUTE_2);
        assertThat("Result has 1 elements", result.size(), is(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge attribute type", mergerResult.getItemType(), is(ItemTypes.EDGE_ATTRIBUTE));
        assertThat("For first result, already exist is returned", mergerResult.getResultType(), is(ProcessingResult.ResultType.NEW_OBJECT));

        EdgeIdentification newEdge = context.getEdgeDbId(MERGED_EDGE[EdgeFormat.ID.getIndex()]);
        assertThat("Edge was not replaced", oldEdge.getRelationId(), is(newEdge.getRelationId()));

        assertThat("Edge creation description was stored.",
                ((ParallelStoredProcessorContext)context).getEdgeQueryList(), hasSize(1));
        DelayedEdgeQuery query = (DelayedEdgeQuery)((ParallelStoredProcessorContext)context)
                .getEdgeQueryList().get(0);
        assertThat("Query key", query.getEdgeType(),
                is(MantaRelationshipType.parseFromGraphType(MERGED_EDGE[EdgeFormat.TYPE.getIndex()])));
        assertThat("Query value", query.getLocalId(), is(MERGED_EDGE[EdgeFormat.ID.getIndex()]));
        assertThat("Query source identifier", query.getSourceNodeId(),
                is(context.getMapNodeIdToDbId().get(MERGED_EDGE[EdgeFormat.SOURCE.getIndex()])));
        assertThat("Query target identifier", query.getTargetNodeId(),
                is(context.getMapNodeIdToDbId().get(MERGED_EDGE[EdgeFormat.TARGET.getIndex()])));

        assertThat("Query has additional properties", query.getProperties().keySet(), hasSize(3));
        assertThat("Edge interpolated property", query.getProperties().get(EdgeProperty.INTERPOLATED.t()),
                is(false));
        assertThat("Edge targetId property", query.getProperties().get(EdgeProperty.TARGET_ID.t()),
                is(context.getMapNodeIdToDbId().get(MERGED_NODE_PARENT[NodeFormat.ID.getIndex()])));
        assertThat("Edge EDGE_PROP property", query.getProperties().get(MERGED_EDGE_ATTRIBUTE[AttributeFormat.KEY.getIndex()]),
                is(MERGED_EDGE_ATTRIBUTE_2[AttributeFormat.VALUE.getIndex()]));


        assertThat("Query in correct revision interval", query.getRevisionInterval(),
                is(new RevisionInterval(2.0, 2.999999)));
    }

    @Override
    @Test
    public void testProcessNodeAttributeSourceCodeNew() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_SOURCE_CODE);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_ATTRIBUTE_SOURCE_CODE);
        assertThat("Result has 1 elements", result.size(), is(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node attribute type", mergerResult.getItemType(), is(ItemTypes.NODE_ATTRIBUTE));
        assertThat("For first result, error is returned", mergerResult.getResultType(), is(ProcessingResult.ResultType.NEW_OBJECT));

        Long nodeId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]);
        String sourceCodeId = context.getMapSourceCodeIdToDbId().get(MERGED_SOURCE_CODE[SourceCodeFormat.ID.getIndex()]);
        Map<String, Object> nodeResult = testGraphUtil.getNodeAttribute(nodeId,
                MERGED_NODE_ATTRIBUTE_SOURCE_CODE[AttributeFormat.KEY.getIndex()], sourceCodeId);

        String[] expected = Arrays.copyOf(MERGED_NODE_ATTRIBUTE_SOURCE_CODE, MERGED_NODE_ATTRIBUTE_SOURCE_CODE.length);
        expected[3] = sourceCodeId;

        assertThat("Node attribute was not created", nodeResult.keySet(), hasSize(0));
        assertThat("Node attribute creation description was stored.",
                ((ParallelStoredProcessorContext)context).getNodeAttributeQueryList(), hasSize(1));
        DelayedNodeAttributeQuery query = (DelayedNodeAttributeQuery)((ParallelStoredProcessorContext)context)
                .getNodeAttributeQueryList().get(0);
        assertThat("Query key", query.getAttributeKey(), is(expected[AttributeFormat.KEY.getIndex()]));
        assertThat("Query value", query.getAttributeValue(), is(expected[AttributeFormat.VALUE.getIndex()]));
        assertThat("Query source identifier", query.getNodeId(), is(nodeId));
        assertThat("Query in correct revision interval", query.getRevisionInterval(), is(REVISION_INTERVAL));
    }

    @Override
    @Test
    public void testProcessEdgeNew() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_PARENT);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_EDGE);
        assertThat("Result has 1 elements", result.size(), is(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of edge type", mergerResult.getItemType(), is(ItemTypes.EDGE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ProcessingResult.ResultType.NEW_OBJECT));

        assertThat("Context edges map has 0 element",
                context.getMapEdgeIdToDbId().keySet(), hasSize(0));
        assertThat("Edge creation description was stored.",
                ((ParallelStoredProcessorContext)context).getEdgeQueryList(), hasSize(1));
        DelayedEdgeQuery query = (DelayedEdgeQuery)((ParallelStoredProcessorContext)context)
                .getEdgeQueryList().get(0);
        assertThat("Query key", query.getEdgeType(),
                is(MantaRelationshipType.parseFromGraphType(MERGED_EDGE[EdgeFormat.TYPE.getIndex()])));
        assertThat("Query value", query.getLocalId(), is(MERGED_EDGE[EdgeFormat.ID.getIndex()]));
        assertThat("Query source identifier", query.getSourceNodeId(),
                is(context.getMapNodeIdToDbId().get(MERGED_EDGE[EdgeFormat.SOURCE.getIndex()])));
        assertThat("Query target identifier", query.getTargetNodeId(),
                is(context.getMapNodeIdToDbId().get(MERGED_EDGE[EdgeFormat.TARGET.getIndex()])));
        assertThat("Query in correct revision interval", query.getRevisionInterval(), is(REVISION_INTERVAL));
    }

    @Override
    @Test
    public void testProcessEdgePerspectiveNew() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);

        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE_2);

        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_PERSPECTIVE_EDGE);
        assertThat("Result has 3 elements", result.size(), is(3));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node attribute type", mergerResult.getItemType(), is(ItemTypes.NODE_ATTRIBUTE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ProcessingResult.ResultType.NEW_OBJECT));
        mergerResult = result.get(1);
        assertThat("First result is of edge attribute type", mergerResult.getItemType(), is(ItemTypes.EDGE_ATTRIBUTE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ProcessingResult.ResultType.NEW_OBJECT));
        mergerResult = result.get(2);
        assertThat("First result is of edge type", mergerResult.getItemType(), is(ItemTypes.EDGE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ProcessingResult.ResultType.NEW_OBJECT));

        Long sourceId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]);
        Long targetId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE_2[NodeFormat.ID.getIndex()]);

        Map<String, Object> edgeResult = testGraphUtil.getRelationship(sourceId, targetId, MantaRelationshipType.PERSPECTIVE);
        Map<String, Object> nodeAttributeResult = testGraphUtil.getAllNodeAttributes(sourceId);

        assertThat("Node attribute was not created", nodeAttributeResult.keySet(), hasSize(0));
        assertThat("Edge was not created", edgeResult.keySet(), hasSize(0));

        assertThat("Edge and node attribute creation description was stored.",
                ((ParallelStoredProcessorContext)context).getEdgeQueryList(), hasSize(1));

        DelayedPerspectiveEdgeQuery query = (DelayedPerspectiveEdgeQuery)((ParallelStoredProcessorContext)context)
                .getEdgeQueryList().get(0);
        DelayedNodeAttributeQuery attributeQuery = query.getNodeAttributeQuery();
        assertThat("Attribute query key", attributeQuery.getAttributeKey(),
                is(MERGED_LAYER_2[LayerFormat.NAME.getIndex()] + " perspective"));
        assertThat("Attribute query value", attributeQuery.getAttributeValue(),
                is(String.format("%s[%s].%s[%s]",
                        MERGED_RESOURCE_2[ResourceFormat.NAME.getIndex()],  MERGED_RESOURCE_2[ResourceFormat.TYPE.getIndex()],
                        MERGED_NODE_RESOURCE_2[NodeFormat.NAME.getIndex()],  MERGED_NODE_RESOURCE_2[NodeFormat.TYPE.getIndex()])));
        assertThat("Attribute query source identifier", attributeQuery.getNodeId(),
                is(sourceId));
        assertThat("Attribute query in correct revision interval", attributeQuery.getRevisionInterval(),
                is(REVISION_INTERVAL));

        assertThat("Edge query key", query.getEdgeType(),
                is(MantaRelationshipType.PERSPECTIVE));
        assertThat("Edge query value", query.getLocalId(),
                is(MERGED_EDGE[EdgeFormat.ID.getIndex()]));
        assertThat("Edge query source identifier", query.getSourceNodeId(),
                is(context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()])));
        assertThat("Edge query target identifier", query.getTargetNodeId(),
                is(context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE_2[NodeFormat.ID.getIndex()])));
        assertThat("Query in correct revision interval", query.getRevisionInterval(), is(REVISION_INTERVAL));
    }
}
