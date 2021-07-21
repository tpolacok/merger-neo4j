package eu.profinit.manta.graphplayground.repository.merger.stored.preprocess;

import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.*;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.NodeFormat;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.ResourceFormat;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.container.MergeContainer;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.phase.MergePhase;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelMergeContainerQueue;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelNodeQueue;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelNodeWithAttributesQueue;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.*;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.RawMergeQuery;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static eu.profinit.manta.graphplayground.repository.merger.stored.preprocess.PreprocessorTestObjects.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests for parallel merging preprocessor.
 *
 * @author tpolacok
 */
public class PreprocessorTest {

    @Test
    public void testNodePreprocessor() {
        ParallelNodeQueue nodeQueue = NodePreprocessor.preprocess(RESOURCES, NODES);
        String root = nodeQueue.poll();
        String root2 = nodeQueue.poll();
        assertThat("First Resource's id is inserted to the queue", root,
                is(RESOURCES.get(0).get(ResourceFormat.ID.getIndex())));
        assertThat("Second Resource's id is inserted to the queue", root2,
                is(RESOURCES.get(1).get(ResourceFormat.ID.getIndex())));

        List<List<String>> owned = nodeQueue.getOwned(root);
        assertThat("Resource has correct amount of children", owned,
                hasSize(2));
        assertThat("Resource has the correct child", owned.get(0),
                equalTo(NODES.get(0)));
        assertThat("Resource has the correct children", owned.get(1),
                equalTo(NODES.get(1)));

        owned = nodeQueue.getOwned(owned.get(1).get(NodeFormat.ID.getIndex()));
        assertThat("Parent has correct amount of children", owned,
                hasSize(1));
        assertThat("Parent has the correct child", owned.get(0),
                equalTo(NODES.get(2)));

        assertThat("Node has different resource",
                nodeQueue.differentResource(owned.get(0).get(NodeFormat.ID.getIndex())),
                is(true));
    }

    @Test
    public void testNodeAttributePreprocessorWithNodes() {
        ParallelNodeQueue nodeQueue = NodePreprocessor.preprocess(RESOURCES, NODES);
        ParallelNodeWithAttributesQueue nodeWithAttributesQueue =
                NodeAttributePreprocessor.preprocessWithNodes(nodeQueue, NODE_ATTRIBUTES);
        String root = nodeWithAttributesQueue.poll();
        List<List<String>> owned = nodeQueue.getOwned(root);
        assertThat("Resource has correct amount of children", owned,
                hasSize(2));
        List<List<String>> ownedNext = nodeWithAttributesQueue.getOwned(owned.get(0).get(NodeFormat.ID.getIndex()));
        assertThat("Node has correct amount of owned nodes", ownedNext,
                hasSize(1));
        ownedNext = nodeWithAttributesQueue.getOwned(owned.get(1).get(NodeFormat.ID.getIndex()));
        assertThat("Resource has correct amount of owned nodes", ownedNext,
                hasSize(2));
        assertThat("MapsTo node attributes stored", nodeWithAttributesQueue.getMapsToAttributes(), hasSize(1));
        assertThat("Correct mapsTo node attributes stored", nodeWithAttributesQueue.getMapsToAttributes().get(0),
                equalTo(NODE_ATTRIBUTES.get(2)));
    }

    @Test
    public void testNodeAttributePreprocessor_initial_2containers() {
        MergePhase phase = NodeAttributePreprocessor.initialSplit(NODE_ATTRIBUTES, 2);
        assertThat("Correct phase size", phase.size(), is(2));

        ParallelMergeContainerQueue queue = phase.getQueueList().get(0);
        ParallelMergeContainerQueue queue2 = phase.getQueueList().get(1);

        assertThat("Correct mapsTo attribute queue list size", queue.getInitialSize(), is(1));
        assertThat("Correct attribute queue list size", queue2.getInitialSize(), is(3));

        MergeContainer container = queue.poll();
        assertThat("Correct attribute size of the first container (mapsTo)",
                container.getContent(),
                hasSize(1));
        assertThat("Correct attribute", ((RawMergeQuery)container.getContent().get(0)).getContent(),
                equalTo(NODE_ATTRIBUTES.get(2)));

        MergeContainer container2 = queue2.poll();
        assertThat("Correct attribute size of first container (other)",
                container2.getContent(),
                hasSize(2));
        assertThat("Correct attribute", ((RawMergeQuery)container2.getContent().get(0)).getContent(),
                equalTo(NODE_ATTRIBUTES.get(0)));
        assertThat("Correct attribute", ((RawMergeQuery)container2.getContent().get(1)).getContent(),
                equalTo(NODE_ATTRIBUTES.get(1)));

        container2 = queue2.poll();
        assertThat("Correct attribute size of the second container (other)",
                container2.getContent(),
                hasSize(1));
        assertThat("Correct attribute", ((RawMergeQuery)container2.getContent().get(0)).getContent(),
                equalTo(NODE_ATTRIBUTES.get(3)));
    }

    @Test
    public void testNodeAttributePreprocessor_initial_3containers() {
        MergePhase phase = NodeAttributePreprocessor.initialSplit(NODE_ATTRIBUTES, 3);
        assertThat("Correct phase size", phase.size(), is(2));

        ParallelMergeContainerQueue queue = phase.getQueueList().get(0);
        ParallelMergeContainerQueue queue2 = phase.getQueueList().get(1);

        assertThat("Correct mapsTo attribute queue list size", queue.getInitialSize(), is(1));
        assertThat("Correct attribute queue list size", queue2.getInitialSize(), is(3));

        MergeContainer container = queue.poll();
        assertThat("Correct attribute size of first container (mapsTo)",
                container.getContent(),
                hasSize(1));
        assertThat("Correct attribute", ((RawMergeQuery)container.getContent().get(0)).getContent(),
                equalTo(NODE_ATTRIBUTES.get(2)));

        MergeContainer container2 = queue2.poll();
        assertThat("Correct attribute size of first container (other)",
                container2.getContent(),
                hasSize(1));
        assertThat("Correct attribute", ((RawMergeQuery)container2.getContent().get(0)).getContent(),
                equalTo(NODE_ATTRIBUTES.get(0)));

        container2 = queue2.poll();
        assertThat("Correct attribute size of the second container (other)",
                container2.getContent(),
                hasSize(1));
        assertThat("Correct attribute", ((RawMergeQuery)container2.getContent().get(0)).getContent(),
                equalTo(NODE_ATTRIBUTES.get(1)));

        container2 = queue2.poll();
        assertThat("Correct attribute size of the third container (other)",
                container2.getContent(),
                hasSize(1));
        assertThat("Correct attribute", ((RawMergeQuery)container2.getContent().get(0)).getContent(),
                equalTo(NODE_ATTRIBUTES.get(3)));
    }

    @Test
    public void testNodeAttributePreprocessor_redistribute_2containers() {
        MergePhase phase = NodeAttributePreprocessor.redistributeNodeAttributesQueries(
                new ArrayList<>(NODE_ATTRIBUTES_DELAYED), 2);
        assertThat("Correct phase size", phase.size(), is(1));

        ParallelMergeContainerQueue queue = phase.getQueueList().get(0);
        assertThat("Correct attribute queue list size", queue.getInitialSize(), is(6));

        MergeContainer container = queue.poll();
        assertThat("Correct attribute size of first container",
                container.getContent(),
                hasSize(3));
        container = queue.poll();
        assertThat("Correct attribute size of second container",
                container.getContent(),
                hasSize(3));
    }

    @Test
    public void testNodeAttributePreprocessor_redistribute_3containers() {
        MergePhase phase = NodeAttributePreprocessor.redistributeNodeAttributesQueries(
                new ArrayList<>(NODE_ATTRIBUTES_DELAYED), 3);
        assertThat("Correct phase size", phase.size(), is(1));

        ParallelMergeContainerQueue queue = phase.getQueueList().get(0);
        assertThat("Correct attribute queue list size", queue.getInitialSize(), is(6));

        MergeContainer container = queue.poll();
        assertThat("Correct attribute size of first container",
                container.getContent(),
                hasSize(3));
        container = queue.poll();
        assertThat("Correct attribute size of second container",
                container.getContent(),
                hasSize(2));
        container = queue.poll();
        assertThat("Correct attribute size of third container",
                container.getContent(),
                hasSize(1));
    }

    @Test
    public void testEdgeAttributePreprocessor() {
        Map<String, List<CommonMergeQuery>> edgeToAttributes =
                EdgeAttributePreprocessor.preprocessForEdges(EDGE_ATTRIBUTES);
        assertThat("Size of edge to edge attributes mapping", edgeToAttributes.keySet(), hasSize(3));
        assertThat("Correct size of edge attributes for an edge",
                edgeToAttributes.get(EDGE_ATTRIBUTES.get(0).get(EdgeFormat.ID.getIndex())),
                hasSize(1));
        assertThat("Correct edge attributes for an edge",
                ((RawMergeQuery)edgeToAttributes.get(EDGE_ATTRIBUTES.get(0).get(EdgeFormat.ID.getIndex())).get(0)).getContent(),
                equalTo(EDGE_ATTRIBUTES.get(0)));

        assertThat("Correct size of edge attributes for an edge",
                edgeToAttributes.get(EDGE_ATTRIBUTES.get(1).get(EdgeFormat.ID.getIndex())),
                hasSize(1));
        assertThat("Correct edge attributes for an edge",
                ((RawMergeQuery)edgeToAttributes.get(EDGE_ATTRIBUTES.get(1).get(EdgeFormat.ID.getIndex())).get(0)).getContent(),
                equalTo(EDGE_ATTRIBUTES.get(1)));

        assertThat("Correct size of edge attributes for an edge",
                edgeToAttributes.get(EDGE_ATTRIBUTES.get(2).get(EdgeFormat.ID.getIndex())),
                hasSize(1));
        assertThat("Correct edge attributes for an edge",
                ((RawMergeQuery)edgeToAttributes.get(EDGE_ATTRIBUTES.get(2).get(EdgeFormat.ID.getIndex())).get(0)).getContent(),
                equalTo(EDGE_ATTRIBUTES.get(2)));
    }

    @Test
    public void testEdgePreprocessor_initial_2containers() {
        MergePhase phase = EdgeNodePreprocessor.initialSplit(EDGES, 2);
        assertThat("Correct phase size", phase.size(), is(2));

        ParallelMergeContainerQueue queue = phase.getQueueList().get(0);
        ParallelMergeContainerQueue queue2 = phase.getQueueList().get(1);

        assertThat("Correct perspective attribute queue list size", queue.getInitialSize(), is(1));
        assertThat("Correct attribute queue list size", queue2.getInitialSize(), is(3));

        MergeContainer container = queue.poll();
        assertThat("Correct attribute size of the first container (perspective)",
                container.getContent(),
                hasSize(1));
        assertThat("Correct attribute", ((RawMergeQuery)container.getContent().get(0)).getContent(),
                equalTo(EDGES.get(3)));

        MergeContainer container2 = queue2.poll();
        assertThat("Correct attribute size of first container (other)",
                container2.getContent(),
                hasSize(2));
        assertThat("Correct attribute", ((RawMergeQuery)container2.getContent().get(0)).getContent(),
                equalTo(EDGES.get(0)));
        assertThat("Correct attribute", ((RawMergeQuery)container2.getContent().get(1)).getContent(),
                equalTo(EDGES.get(1)));

        container2 = queue2.poll();
        assertThat("Correct attribute size of the second container (other)",
                container2.getContent(),
                hasSize(1));
        assertThat("Correct attribute", ((RawMergeQuery)container2.getContent().get(0)).getContent(),
                equalTo(EDGES.get(2)));
    }

    @Test
    public void testEdgePreprocessor_initial_3containers() {
        MergePhase phase = EdgeNodePreprocessor.initialSplit(EDGES, 3);
        assertThat("Correct phase size", phase.size(), is(2));

        ParallelMergeContainerQueue queue = phase.getQueueList().get(0);
        ParallelMergeContainerQueue queue2 = phase.getQueueList().get(1);

        assertThat("Correct perspective attribute queue list size", queue.getInitialSize(), is(1));
        assertThat("Correct attribute queue list size", queue2.getInitialSize(), is(3));

        MergeContainer container = queue.poll();
        assertThat("Correct attribute size of first container (perspective)",
                container.getContent(),
                hasSize(1));
        assertThat("Correct attribute", ((RawMergeQuery)container.getContent().get(0)).getContent(),
                equalTo(EDGES.get(3)));

        MergeContainer container2 = queue2.poll();
        assertThat("Correct attribute size of first container (other)",
                container2.getContent(),
                hasSize(1));
        assertThat("Correct attribute", ((RawMergeQuery)container2.getContent().get(0)).getContent(),
                equalTo(EDGES.get(0)));

        container2 = queue2.poll();
        assertThat("Correct attribute size of the second container (other)",
                container2.getContent(),
                hasSize(1));
        assertThat("Correct attribute", ((RawMergeQuery)container2.getContent().get(0)).getContent(),
                equalTo(EDGES.get(1)));

        container2 = queue2.poll();
        assertThat("Correct attribute size of the third container (other)",
                container2.getContent(),
                hasSize(1));
        assertThat("Correct attribute", ((RawMergeQuery)container2.getContent().get(0)).getContent(),
                equalTo(EDGES.get(2)));
    }

    @Test
    public void testEdgeNodePreprocessor_redistribute_2containers() {
        List<ParallelMergeContainerQueue> phases = EdgeNodePreprocessor.redistributeEdgeQueries(
                EDGES_DELAYED, Map.of(),2);
        assertThat("Correct phase size", phases, hasSize(2));

        ParallelMergeContainerQueue queue = phases.get(0);
        assertThat("Correct edge queue list size", queue.getInitialSize(), is(6));

        MergeContainer container = queue.poll();
        assertThat("Correct attribute size of first container",
                container.getContent(),
                hasSize(3));
        container = queue.poll();
        assertThat("Correct attribute size of second container",
                container.getContent(),
                hasSize(3));
    }

    @Test
    public void testEdgeNodePreprocessor_redistribute_3containers() {
        List<ParallelMergeContainerQueue> phases = EdgeNodePreprocessor.redistributeEdgeQueries(
                EDGES_DELAYED, Map.of(),3);
        assertThat("Correct phase size", phases, hasSize(2));

        ParallelMergeContainerQueue queue = phases.get(0);
        assertThat("Correct edge queue list size", queue.getInitialSize(), is(6));

        MergeContainer container = queue.poll();
        assertThat("Correct attribute size of first container",
                container.getContent(),
                hasSize(3));
        container = queue.poll();
        assertThat("Correct attribute size of second container",
                container.getContent(),
                hasSize(3));
        container = queue.poll();
        assertThat("Correct attribute size of third container",
                container.getContent(),
                hasSize(0));
    }

    @Test
    public void testEdgeNeighborhoodPreprocessor_redistribute_2containers() {
        List<ParallelMergeContainerQueue> phases = EdgeNeighborhoodPreprocessor.preprocess(
                RAW_MERGE_EDGES, new HashMap<>(), new HashMap<>(), 2, false);
        assertThat("Correct phase size", phases, hasSize(2));

        ParallelMergeContainerQueue queue = phases.get(0);
        assertThat("Correct edge queue list size", queue.getInitialSize(), is(6));

        MergeContainer container = queue.poll();
        assertThat("Correct attribute size of first container",
                container.getContent(),
                hasSize(3));
        container = queue.poll();
        assertThat("Correct attribute size of second container",
                container.getContent(),
                hasSize(3));
    }
}
