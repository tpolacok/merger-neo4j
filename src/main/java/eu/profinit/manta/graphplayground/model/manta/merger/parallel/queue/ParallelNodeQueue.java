package eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue;

import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Concurrent node queue for concurrent processing of node hierarchy.
 *
 * @author tpolacok
 */
public class ParallelNodeQueue {

    /**
     * Blocking queue containing available nodes (ids) to process
     */
    private BlockingQueue<String> nodeQueue;

    /**
     * Mapping of node's id to list of children merge definitions.
     */
    private Map<String, List<List<String>>> nodeToChildren;

    /**
     * Set of possible nodes with different resource-parent's resource
     */
    private Set<String> resourceNodes;

    /**
     * Delayed node queries
     */
    private BlockingQueue<CommonMergeQuery> delayedQueries = new LinkedBlockingDeque<>();

    /**
     * Constructor for node queue
     * @param nodeToChildren Mapping of node's id to list of children merge definitions.
     */
    public ParallelNodeQueue(Map<String, List<List<String>>> nodeToChildren,
                             Set<String> resourceNodes,
                             List<String> initList) {
        this.nodeToChildren = nodeToChildren;
        this.nodeQueue = new LinkedBlockingDeque<>(initList);
        this.resourceNodes = resourceNodes;
    }

    /**
     * Shallow copy constructor.
     * @param other Other node queue.
     */
    public ParallelNodeQueue(ParallelNodeQueue other) {
        this.nodeToChildren = other.nodeToChildren;
        this.nodeQueue = other.nodeQueue;
        this.resourceNodes = other.resourceNodes;
    }

    /**
     * Gets list of nodes belonging to node.
     * @param nodeId Id of node.
     * @return List of objects to process.
     */
    public List<List<String>> getOwned(String nodeId) {
        return nodeToChildren.getOrDefault(nodeId, Collections.emptyList());
    }

    /**
     * Flag whether node has different resource than its parent node.
     * @param nodeId Id of the node.
     * @return Boolean flag indicating node's assignment to different resource than its parent.
     */
    public boolean differentResource(String nodeId) {
        return resourceNodes.contains(nodeId);
    }

    /**
     * Adds query to synchronized list - to be processed later.
     * @param query Delayed nodeq query
     */
    public void addResourceProcessing(CommonMergeQuery query) {
        delayedQueries.add(query);
    }

    /**
     * @return Next query to process
     */
    public CommonMergeQuery getNextDelayedMergeQuery() {
        return delayedQueries.poll();
    }

    /**
     * Adds new node id to the queue.
     * @param nodeId Id of the node.
     */
    public void addToQueue(String nodeId) {
        nodeQueue.add(nodeId);
    }

    /**
     * Polls node from the queue.
     * @return Node or null if queue is empty.
     */
    public String poll() {
        return nodeQueue.poll();
    }

}
