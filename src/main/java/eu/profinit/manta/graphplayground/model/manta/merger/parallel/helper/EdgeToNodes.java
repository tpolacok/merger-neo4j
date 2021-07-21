package eu.profinit.manta.graphplayground.model.manta.merger.parallel.helper;

/**
 * Source and target node's id for edge.
 *
 * @author tpolacok.
 */
public class EdgeToNodes {

    /**
     * Local id for edge.
     */
    private String edgeLocalId;

    /**
     * Edge's source node id.
     */
    private String sourceNodeId;

    /**
     * Edge's target node id.
     */
    private String targetNodeId;

    /**
     * Constructor
     * @param edgeLocalId Local id for edge.
     * @param sourceNodeId Edge's source node id.
     * @param targetNodeId Edge's target node id.
     */
    public EdgeToNodes(String edgeLocalId, String sourceNodeId, String targetNodeId) {
        this.edgeLocalId = edgeLocalId;
        this.sourceNodeId = sourceNodeId;
        this.targetNodeId = targetNodeId;
    }

    /**
     * @return Local id for edge.
     */
    public String getEdgeLocalId() {
        return edgeLocalId;
    }

    /**
     * @return Edge's source node id.
     */
    public String getSourceNodeId() {
        return sourceNodeId;
    }

    /**
     * @return Edge's target node id.
     */
    public String getTargetNodeId() {
        return targetNodeId;
    }
}
