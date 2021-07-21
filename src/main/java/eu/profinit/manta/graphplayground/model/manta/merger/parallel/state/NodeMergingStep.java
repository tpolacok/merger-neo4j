package eu.profinit.manta.graphplayground.model.manta.merger.parallel.state;

/**
 * Helper class for node merging.
 *
 * @author tpolacok
 */
public class NodeMergingStep {
    /**
     * State of merging thread.
     */
    private NodeMergingState state;

    /**
     * Node id to be processed.
     */
    private String nodeId;

    /**
     * @param state State of merging thread.
     * @param nodeId Node id to be processed.
     */
    public NodeMergingStep(NodeMergingState state, String nodeId) {
        this.state = state;
        this.nodeId = nodeId;
    }

    /**
     * Constructor for merging step.
     * @param state State of node merging.
     * @param nodeId Id of node to be processed.
     * @return new NodeMergingStep.
     */
    public static NodeMergingStep of(NodeMergingState state, String nodeId) {
        return new NodeMergingStep(state, nodeId);
    }

    /**
     * Constructor for merging step.
     * @param state State of node merging.
     * @return new NodeMergingStep.
     */
    public static NodeMergingStep of(NodeMergingState state) {
        return new NodeMergingStep(state, null);
    }

    /**
     * @return State.
     */
    public NodeMergingState getState() {
        return state;
    }

    /**
     * @return Node id.
     */
    public String getNodeId() {
        return nodeId;
    }
}
