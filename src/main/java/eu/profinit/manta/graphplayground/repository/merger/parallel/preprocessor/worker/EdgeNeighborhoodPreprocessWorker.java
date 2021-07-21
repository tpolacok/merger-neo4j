package eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.worker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Phaser;
import java.util.stream.Collectors;

import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelMergeContainerQueue;
import org.neo4j.logging.Log;

import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.EdgeRawMergeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.EdgeNeighborhoodPreprocessor;

/**
 * Preprocesses edges and edge attributes.
 *
 * Edges are split into several containers, where sets of nodes of each container (source and target nodes of edges)
 * are always non-intersecting, but also the neighborhood of the nodes (adjacent nodes) do not belong to either container.
 *
 * @author tpolacok
 */
public class EdgeNeighborhoodPreprocessWorker extends EdgePreprocessWorker {

    /**
     * Mapping of node to adjacent nodes.
     */
    private Map<String, Set<String>> nodeToAdjacentNodes;

    /**
     * @param edges List of edges.
     * @param nodes List of nodes.
     * @param edgeAttributes List of edge attributes.
     * @param edgeQueues List of synchronized edge queues.
     * @param phaser Phaser for synchronization of merging.
     * @param threads Number of threads.
     * @param LOGGER Logger instance.
     */
    public EdgeNeighborhoodPreprocessWorker(List<List<String>> edges, List<List<String>> nodes,
                                            List<List<String>> edgeAttributes,
                                            List<ParallelMergeContainerQueue> edgeQueues,
                                            Phaser phaser, int threads, Log LOGGER) {
        super(edges, nodes, edgeAttributes, edgeQueues, phaser, threads, LOGGER);
        nodeToAdjacentNodes = initializeAdjacency();
    }

    @Override
    protected List<ParallelMergeContainerQueue> getNext(List<CommonMergeQuery> edges,
                                                        Map<String, List<CommonMergeQuery>> edgeToAttributes) {
        return EdgeNeighborhoodPreprocessor.preprocess(edges, nodeToAdjacentNodes,
                edgeToAttributes, threads, false);
    }

    @Override
    protected List<ParallelMergeContainerQueue> getInitial(Map<String, List<CommonMergeQuery>> edgeToAttributes) {
        return EdgeNeighborhoodPreprocessor.preprocess(edges.stream().map(EdgeRawMergeQuery::new).collect(Collectors.toList()),
                nodeToAdjacentNodes,
                edgeToAttributes, threads, true);
    }

    /**
     * Initialize neighborhoods of nodes by nodes.
     * @return Initialized mapping of node it to neighboring nodes ids.
     */
    private Map<String, Set<String>> initializeAdjacency() {
        Map<String, String> nodeToResource = new HashMap<>();
        Map<String, Set<String>> nodeToAdjacentNodes = new HashMap<>();
        for (List<String> node: nodes) {
            nodeToResource.put(node.get(1), node.get(5));
            String parent;
            if (node.get(2).equals("")) {
                parent = node.get(5);
            } else {
                parent = node.get(2);
                if (!node.get(5).equals(nodeToResource.get(parent))) {
                    nodeToAdjacentNodes.computeIfAbsent(node.get(1), key -> new HashSet<>()).add(node.get(5));
                    nodeToAdjacentNodes.computeIfAbsent(node.get(5), key -> new HashSet<>()).add(node.get(1));
                }
            }
            nodeToAdjacentNodes.computeIfAbsent(node.get(1), key -> new HashSet<>()).add(parent);
            nodeToAdjacentNodes.computeIfAbsent(parent, key -> new HashSet<>()).add(node.get(1));
        }
        return nodeToAdjacentNodes;
    }

}
