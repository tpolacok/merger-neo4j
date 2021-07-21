package eu.profinit.manta.graphplayground.model.manta.merger.parallel.state;

/**
 * State of node merging.
 *
 * @author tpolacok
 */
public enum NodeMergingState {
    /**
     * Processing of nodes is done.
     */
    PROCESSING_DONE,
    /**
     * Processing of nodes is not done.
     */
    PROCESSING_CONTINUE,
    /**
     * Process next node.
     */
    PROCESSING_NODE
}
