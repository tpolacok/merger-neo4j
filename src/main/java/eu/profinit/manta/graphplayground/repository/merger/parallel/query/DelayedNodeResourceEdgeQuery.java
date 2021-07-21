package eu.profinit.manta.graphplayground.repository.merger.parallel.query;

import java.util.List;

import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;

/**
 * Represents delayed node-resource relationship creation query.
 *
 * @author tpolacok.
 */
public class DelayedNodeResourceEdgeQuery implements CommonMergeQuery {

    /**
     * Id of the node.
     */
    private final String nodeId;

    /**
     * Id of the resource.
     */
    private final String resourceNodeId;

    /**
     * @param nodeId Id of the node.
     * @param resourceNodeId Id of the resource.
     */
    public DelayedNodeResourceEdgeQuery(String nodeId, String resourceNodeId) {
        this.nodeId = nodeId;
        this.resourceNodeId = resourceNodeId;
    }

    /**
     * @return Id of the node.
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * @return Id of the resource
     */
    public String getResourceNodeId() {
        return resourceNodeId;
    }


    @Override
    public List<MergerProcessorResult> evaluate(StoredMergerProcessor mergerProcessor, StoredProcessorContext context) {
        mergerProcessor.processNodeResourceRelationship(this, context);
        return List.of();
    }
}

