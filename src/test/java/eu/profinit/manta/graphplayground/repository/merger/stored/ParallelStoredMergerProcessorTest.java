package eu.profinit.manta.graphplayground.repository.merger.stored;

import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.context.ParallelStoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.merger.parallel.processor.ParallelStoredMergerProcessor;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphCreation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphOperation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSourceRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSuperRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionUtils;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static eu.profinit.manta.graphplayground.repository.merger.common.CommonMergerTestObjects.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class ParallelStoredMergerProcessorTest extends StoredMergerProcessorTest {

    @Override
    protected void beforeEachInit(Double mergedRevision, Double latestCommittedRevision) {
        testGraphUtil = new StoredTestGraphUtil(driver, neo4j.defaultDatabaseService());
        // Init merger processor
        StoredRevisionUtils revisionUtils = new StoredRevisionUtils();
        StoredGraphOperation graphOperation = new StoredGraphOperation(revisionUtils);
        StoredGraphCreation graphCreation = new StoredGraphCreation(graphOperation);
        context = new ParallelStoredProcessorContext(mergedRevision, latestCommittedRevision);
        initialProcessor = new StoredMergerProcessor(
                new StoredSuperRootHandler(), new StoredSourceRootHandler(graphOperation), new StoredRevisionRootHandler(),
                graphOperation, graphCreation, revisionUtils, LOGGER
        );

        testedProcessor = new ParallelStoredMergerProcessor(
                new StoredSuperRootHandler(), new StoredSourceRootHandler(graphOperation), new StoredRevisionRootHandler(),
                graphOperation, graphCreation, revisionUtils, LOGGER
        );
    }

    @Override
    @Test
    public void testProcessNodeNewResourceAndParentDifferent() {
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER);
        testGraphUtil.merge(initialProcessor, context, MERGED_LAYER_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE);
        testGraphUtil.merge(initialProcessor, context, MERGED_RESOURCE_2);
        testGraphUtil.merge(initialProcessor, context, MERGED_NODE_RESOURCE);
        List<MergerProcessorResult> result = testGraphUtil.merge(testedProcessor, context, MERGED_NODE_PARENT_DIFFERENT_RESOURCE);
        assertThat("Result has 1 elements", result.size(), is(1));
        MergerProcessorResult mergerResult = result.get(0);
        assertThat("First result is of node type", mergerResult.getItemType(), is(DataflowObjectFormats.ItemTypes.NODE));
        assertThat("For first result, new object is returned", mergerResult.getResultType(), is(ProcessingResult.ResultType.NEW_OBJECT));

        assertThat("Context node local-database map has 2 elements",
                context.getMapNodeIdToDbId().keySet(), hasSize(2));
        assertThat("Context node local-database map has the new node",
                context.getMapNodeIdToDbId().containsKey(MERGED_NODE_PARENT[DataflowObjectFormats.NodeFormat.ID.getIndex()]));

        Long dbId = context.getMapNodeIdToDbId().get(MERGED_NODE_PARENT[DataflowObjectFormats.NodeFormat.ID.getIndex()]);
        assertThat("Context node-resource map has 4 elements",
                context.getMapNodeDbIdResourceDbId().keySet(), hasSize(4));
        assertThat("Context node-resource map has the nodeId",
                context.getMapNodeDbIdResourceDbId(), hasKey(dbId));

        Long resourceId = context.getMapResourceIdToDbId().get(MERGED_RESOURCE_2[DataflowObjectFormats.ResourceFormat.ID.getIndex()]);
        assertThat("Context node-resource has correct resource for new node",
                context.getMapNodeDbIdResourceDbId().get(dbId), is(resourceId));

        Long parentId = context.getMapNodeIdToDbId().get(MERGED_NODE_RESOURCE[DataflowObjectFormats.NodeFormat.ID.getIndex()]);

        Map<String, Object> nodeResult = testGraphUtil.getNodeByIdParentResource(dbId, parentId, resourceId);
        assertThat("No node-resource relationship should be created",
                nodeResult.size(), is(0));
    }
}