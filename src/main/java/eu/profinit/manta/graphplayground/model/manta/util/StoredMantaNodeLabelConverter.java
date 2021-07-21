package eu.profinit.manta.graphplayground.model.manta.util;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;

public class StoredMantaNodeLabelConverter implements MantaNodeLabelConverter {

    /**
     * Retrieves {@link MantaNodeLabel} of the given node.
     * @param vertex Input node.
     * @return {@link MantaNodeLabel} label
     */
    public static MantaNodeLabel getType(Node vertex) {
        if (vertex == null) {
            throw new IllegalArgumentException("The argument is null.");
        }
        MantaNodeLabel nodeLabel = null;
        for (Label value : vertex.getLabels()) {
            String label = value.name();
            if (MantaNodeLabel.MANTA_NODE_LABEL_CONVERSION_MAP.containsKey(label)) {
                nodeLabel = MantaNodeLabel.MANTA_NODE_LABEL_CONVERSION_MAP.get(label);
                break;
            }
        }
        if (nodeLabel != null) {
            return nodeLabel;
        } else {
            throw new IllegalStateException(
                    "Unknown type of vertex. Id: " + vertex.getId() + "; attrs: " + vertex.getAllProperties().keySet() + ".");
        }
    }
}
