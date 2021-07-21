package eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.NodeFormat;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelNodeQueue;

/**
 * Preprocessor for nodes and resources.
 *
 * @author tpolacok
 */
public class NodePreprocessor {

    /**
     * Preprocesses input file by storing nodes parent to child relationships.
     * @param resources List of merged resources.
     * @param nodes List of merged nodes.
     * @return Mapping of node's local id to its children local ids.
     */
    public static ParallelNodeQueue preprocess(List<List<String>> resources, List<List<String>> nodes) {
        // Set of possible nodes with different resource-parent's resource
        Set<String> resourceNodes = new HashSet<>();
        // Mapping of node id to resource id
        Map<String, String> nodeToResource = new HashMap<>();
        // Mapping of node id to children merge list
        Map<String, List<List<String>>> parentToChildren = new HashMap<>();
        List<String> init = new ArrayList<>();
        for (List<String> resource: resources) {
            init.add(resource.get(NodeFormat.ID.getIndex()));
        }
        for (List<String> node: nodes) {
            nodeToResource.put(node.get(NodeFormat.ID.getIndex()), node.get(NodeFormat.RESOURCE_ID.getIndex()));
            if (node.get(NodeFormat.PARENT_ID.getIndex()).equals("")) {
                parentToChildren.computeIfAbsent(node.get(NodeFormat.RESOURCE_ID.getIndex()), k -> new ArrayList<>()).add(node);
            } else {
                if (!node.get(NodeFormat.RESOURCE_ID.getIndex()).equals(nodeToResource.get(node.get(NodeFormat.PARENT_ID.getIndex())))) {
                    resourceNodes.add(node.get(NodeFormat.ID.getIndex()));
                }
                parentToChildren.computeIfAbsent(node.get(NodeFormat.PARENT_ID.getIndex()), k -> new ArrayList<>()).add(node);
            }
        }
        return new ParallelNodeQueue(parentToChildren, resourceNodes, init);
    }
}
