package eu.profinit.manta.graphplayground.model.manta.merger;

/**
 * Describes type of merging.
 *
 * @author tpolacok
 */
public enum MergerType {
    /**
     * Merge is done on client side.
     */
    CLIENT,
    /**
     * Merge is done on server side (stored procedure) done sequentially.
     */
    SERVER_SEQUENTIAL,
    /**
     * Merge is done on server side (stored procedure) done in parallel.
     * Nodes are processed as part of shared queue.
     * Attributes are split by nodes into several distinct containers.
     * Edges and edge attributes are split by nodes into several distinct containers.
     */
    SERVER_PARALLEL_A,
    /**
     * Merge is done on server side (stored procedure) done in parallel in a more deadlock-free way.
     * Nodes + their attributes are processed as part of shared queue.
     * Edges and edge attributes are split by nodes and their neighborhoods into several distinct containers.
     */
    SERVER_PARALLEL_B,
}
