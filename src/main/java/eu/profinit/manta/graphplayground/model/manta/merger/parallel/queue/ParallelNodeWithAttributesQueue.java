package eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Node queue with mapping of nodes to attributes.
 *
 * @author tpolacok.
 */
public class ParallelNodeWithAttributesQueue extends ParallelNodeQueue {

    /**
     * Mapping of node to its attributes.
     */
    private Map<String, List<List<String>>> nodeToAttributesMap;

    /**
     * Maps to attributes.
     */
    private List<List<String>> mapToAttributes;

    /**
     * Constructor for node queue
     *
     * @param nodeQueue Synchronized node queue.
     */
    public ParallelNodeWithAttributesQueue(ParallelNodeQueue nodeQueue,
                                           Map<String, List<List<String>>> attributeMap,
                                           List<List<String>> mapToAttributes) {
        super(nodeQueue);
        this.nodeToAttributesMap = attributeMap;
        this.mapToAttributes = mapToAttributes;
    }

    /**
     * Gets list of children and attributes belonging to node.
     * @param nodeId Id of node.
     * @return List of objects to process.
     */
    @Override
    public List<List<String>> getOwned(String nodeId) {
        return Stream.concat(
                    super.getOwned(nodeId).stream(),
                    getNodesAttributes(nodeId).stream())
                .collect(Collectors.toList());
    }

    /**
     *
     * @param nodeId Node id.
     * @return Attributes of the node by its id.
     */
    private List<List<String>> getNodesAttributes(String nodeId) {
        return nodeToAttributesMap.getOrDefault(nodeId, Collections.emptyList());
    }

    /**
     *
     * @return Maps To attributes.
     */
    public List<List<String>> getMapsToAttributes() {
        return mapToAttributes;
    }




}
