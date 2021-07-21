package eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.worker;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Phaser;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.MantaRelationshipType;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.container.MergeContainer;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelMergeContainerQueue;
import org.neo4j.logging.Log;

import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.EdgeAttributePreprocessor;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;
import eu.profinit.manta.graphplayground.repository.stored.merger.StoredMerger;

/**
 * Preprocesses edges and edge attributes.
 *
 * Splitting is done recursively on the rest of edges until size of the rest of the edges is below threshold or
 * sum of split edges is below threshold.
 *
 * During the first split {@link MantaRelationshipType#PERSPECTIVE} edges are filtered as they have to be processed separately.
 *
 * @author tpolacok
 */
public abstract class EdgePreprocessWorker implements Runnable {

    /**
     * Logger instance.
     */
    protected Log LOGGER;

    /**
     * List of merge container queues.
     */
    protected List<ParallelMergeContainerQueue> edgeQueues;

    /**
     * List of merged nodes.
     */
    protected List<List<String>> nodes;

    /**
     * List of merged edges.
     */
    protected List<List<String>> edges;

    /**
     * List of merged edge attributes.
     */
    protected List<List<String>> edgeAttributes;

    /**
     * Number of threads.
     */
    protected int threads;

    /**
     * Phaser instance.
     */
    private final Phaser phaser;

    /**
     * @param edges List of edges.
     * @param nodes List of nodes.
     * @param edgeAttributes List of edge attributes.
     * @param edgeQueues List of synchronized edge queues.
     * @param phaser Phaser for synchronization of merging.
     * @param threads Number of threads.
     * @param LOGGER Logger instance.
     */
    public EdgePreprocessWorker(List<List<String>> edges,
                                List<List<String>> nodes,
                                List<List<String>> edgeAttributes,
                                List<ParallelMergeContainerQueue> edgeQueues,
                                Phaser phaser, int threads, Log LOGGER) {
        this.edges = edges;
        this.nodes = nodes;
        this.edgeAttributes = edgeAttributes;
        this.edgeQueues = edgeQueues;
        this.phaser = phaser;
        this.threads = threads;
        this.LOGGER = LOGGER;
        this.phaser.register();
    }

    @Override
    public void run() {
        Map<String, List<CommonMergeQuery>> edgeToAttributes
                = EdgeAttributePreprocessor.preprocessForEdges(edgeAttributes);
        List<ParallelMergeContainerQueue> result = getInitial(edgeToAttributes);
        edgeQueues.add(result.get(0));
        ParallelMergeContainerQueue perspectiveEdges = result.get(2);
        List<CommonMergeQuery> restOfEdges = result.get(1).poll().getContent();
        int lastSize = restOfEdges.size();
        while (true) {
            restOfEdges = split(restOfEdges, edgeToAttributes);
            int newSize = restOfEdges.size();
            if (newSize == lastSize || newSize < StoredMerger.EDGE_REMAINING_SPLIT_THRESHOLD
                    || edgeQueues.get(edgeQueues.size() - 1).getInitialSize() < StoredMerger.EDGE_COUNT_SPLIT_THRESHOLD)
                break;
            lastSize = newSize;
        }
        edgeQueues.add(new ParallelMergeContainerQueue(Collections.singletonList(new MergeContainer(restOfEdges))));
        edgeQueues.add(perspectiveEdges);

        LOGGER.info("Done preprocessing edges.");
        phaser.arriveAndAwaitAdvance();
    }



    /**
     * Preprocesses edges and inserts edge queue into final list.
     * @param edges Edges to be preprocessed.
     * @param edgeToAttributes Mapping of edge to its attributes.
     * @return Unassigned edges.
     */
    private List<CommonMergeQuery> split(List<CommonMergeQuery> edges, Map<String, List<CommonMergeQuery>> edgeToAttributes) {
        List<ParallelMergeContainerQueue> queues = getNext(edges, edgeToAttributes);
        edgeQueues.add(queues.get(0));
        MergeContainer edgeContainer = queues.get(1).poll();
        return edgeContainer.getContent();
    }

    protected abstract List<ParallelMergeContainerQueue> getNext(List<CommonMergeQuery> edges,
                                                                 Map<String, List<CommonMergeQuery>> edgeToAttributes);

    protected abstract List<ParallelMergeContainerQueue> getInitial(Map<String, List<CommonMergeQuery>> edgeToAttributes);
}
