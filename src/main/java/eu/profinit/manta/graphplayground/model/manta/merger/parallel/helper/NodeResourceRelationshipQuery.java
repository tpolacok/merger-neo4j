package eu.profinit.manta.graphplayground.model.manta.merger.parallel.helper;

/**
 * Represents query for node-resource relationship.
 *
 * @author tpolacok
 */
public class NodeResourceRelationshipQuery {

    /**
     * Id of the node.
     */
    private String nodeId;

    /**
     * Id of the resource.
     */
    private String resourceNodeId;

    /**
     * @param nodeId Id of the node.
     * @param resourceNodeId Id of the resource.
     */
    public NodeResourceRelationshipQuery(String nodeId, String resourceNodeId) {
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
}
