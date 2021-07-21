package eu.profinit.manta.graphplayground.repository.stored.merger.connector;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import org.apache.commons.lang3.Validate;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

/**
 * Class containing methods for working with graph objects.
 * Contains methods for creating and modifying graph objects.
 *
 * @author tpolacok
 */
@Repository
public class StoredGraphCreation {

    /**
     * Graph operation instance.
     */
    private StoredGraphOperation graphOperation;

    /**
     * @param graphOperation Graph operation instance.
     */
    public StoredGraphCreation(StoredGraphOperation graphOperation) {
        this.graphOperation = graphOperation;
    }

    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(StoredGraphCreation.class);

    /**
     * Source node is created with given attributes and is linked from source root
     * @param tx Database transaction
     * @param sourceRoot Source root node
     * @param scLocalName Source code local name
     * @param scHash Source code hash
     * @param scTechnology Source code technology
     * @param scConnection Source code connection
     * @param revisionInterval Source code revision interval
     * @return Created source code vertex.
     */
    public Node createSourceCode(Transaction tx, Node sourceRoot, String scLocalName, String scHash,
                                        String scTechnology, String scConnection, RevisionInterval revisionInterval) {
        Validate.notNull(sourceRoot, "The argument 'sourceRoot' was null.");
        Validate.notNull(revisionInterval, "The argument 'revisionInterval' was null.");
        String id = UUID.randomUUID().toString().replace("-", "").toUpperCase();
        // Edge properties
        Map<String, Object> edgeProps = new HashMap<>();
        edgeProps.put(DatabaseStructure.EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        edgeProps.put(DatabaseStructure.EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
        edgeProps.put(DatabaseStructure.EdgeProperty.SOURCE_LOCAL_NAME.t(), graphOperation.processValueForLocalName(scLocalName));
        // Node properties
        Map<String, Object> vertexProps = new HashMap<>();
        vertexProps.put(DatabaseStructure.NodeProperty.VERTEX_TYPE.t(), MantaNodeLabel.SOURCE_NODE.toString());
        vertexProps.put(DatabaseStructure.NodeProperty.SOURCE_NODE_ID.t(), id);
        vertexProps.put(DatabaseStructure.NodeProperty.SOURCE_NODE_LOCAL.t(), scLocalName);
        vertexProps.put(DatabaseStructure.NodeProperty.SOURCE_NODE_HASH.t(), scHash);
        vertexProps.put(DatabaseStructure.NodeProperty.SOURCE_NODE_TECHNOLOGY.t(), scTechnology);
        vertexProps.put(DatabaseStructure.NodeProperty.SOURCE_NODE_CONNECTION.t(), scConnection);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idSource", sourceRoot.getId());
        params.put("edgeProps", edgeProps);
        params.put("vertexProps", vertexProps);
        Result result = tx.execute(String.format("MATCH (source) WHERE ID(source)=$idSource" +
                        " CREATE (source)-[edge:%s $edgeProps]->" +
                        "(scNode:%s $vertexProps)" +
                        " RETURN scNode",
                DatabaseStructure.MantaRelationshipType.HAS_SOURCE.t(), MantaNodeLabel.SOURCE_NODE.getVertexLabel()), params);
        return result.hasNext()? (Node)result.next().get("scNode"): null;
    }

    /**
     * Creates control edge with given label between source and target node
     * @param tx Database transaction
     * @param source Source vertex
     * @param target Target vertex
     * @param label Edge label
     * @param startRevision Start revision
     * @param endRevision End revision
     */
    public void createControlEdge(Transaction tx, Node source, Node target, String label,
                                         Double startRevision, Double endRevision) {
        Map<String, Object> edgeProps = new HashMap<>();
        edgeProps.put(DatabaseStructure.EdgeProperty.TRAN_START.t(), startRevision);
        edgeProps.put(DatabaseStructure.EdgeProperty.TRAN_END.t(), endRevision);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("edgeProps", edgeProps);
        params.put("idSource", source.getId());
        params.put("idTarget", target.getId());
        tx.execute( String.format("MATCH (source), (target) WHERE ID(source)=$idSource AND ID(target)=$idTarget" +
                        " CREATE (source)-[edge:%s $edgeProps]->(target)", label), params);
    }

    /**
     * Creates node linked to single parent/resource based on the label
     * @param tx Database transaction
     * @param parentId Id of parent node
     * @param label Edge label {@link DatabaseStructure.MantaRelationshipType#HAS_PARENT} or {@link DatabaseStructure.MantaRelationshipType#HAS_RESOURCE}
     * @param name Name of the node
     * @param type Type of the node
     * @param revisionInterval Revision interval.
     * @return Created node vertex.
     */
    public Node createNode(Transaction tx, Long parentId, String label, String name, String type,
                           RevisionInterval revisionInterval) {
        Validate.notNull(parentId, "The argument 'parentId' was null.");
        Validate.notNull(revisionInterval, "The argument 'revisionInterval' was null.");
        // Edge properties
        Map<String, Object> edgeProps = new HashMap<>();
        edgeProps.put(DatabaseStructure.EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        edgeProps.put(DatabaseStructure.EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
        edgeProps.put(DatabaseStructure.EdgeProperty.CHILD_NAME.t(), graphOperation.processValueForChildName(name));
        // Node properties
        Map<String, Object> vertexProps = new HashMap<>();
        vertexProps.put(DatabaseStructure.NodeProperty.VERTEX_TYPE.t(), MantaNodeLabel.NODE.toString());
        vertexProps.put(DatabaseStructure.NodeProperty.NODE_NAME.t(), name);
        vertexProps.put(DatabaseStructure.NodeProperty.NODE_TYPE.t(), type);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idParent", parentId);
        params.put("vertexProps", vertexProps);
        params.put("edgeProps", edgeProps);
        Result result = tx.execute( String.format("MATCH (parent) WHERE ID(parent)=$idParent" +
                        " CREATE (node:%s $vertexProps)-[edge:%s $edgeProps]->(parent)" +
                        " RETURN node", MantaNodeLabel.NODE.getVertexLabel(), label), params);
        return result.hasNext()? (Node)result.next().get("node"): null;
    }

    /**
     * Creates node linked to parent and resource
     * @param tx Database transaction
     * @param parentId Id of parent node
     * @param resourceId Id of resource node
     * @param name Name of the node
     * @param type Type of the node
     * @param revisionInterval Revision interval.
     * @return Created node vertex.
     */
    public Node createNode(Transaction tx, Long parentId, Long resourceId, String name, String type,
                           RevisionInterval revisionInterval) {
        Validate.notNull(parentId, "The argument 'parentId' was null.");
        Validate.notNull(resourceId, "The argument 'resourceId' was null.");
        Validate.notNull(revisionInterval, "The argument 'revisionInterval' was null.");
        // Edge properties
        Map<String, Object> edgeProps = new HashMap<>();
        edgeProps.put(DatabaseStructure.EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        edgeProps.put(DatabaseStructure.EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
        edgeProps.put(DatabaseStructure.EdgeProperty.CHILD_NAME.t(), graphOperation.processValueForChildName(name));
        // Node properties
        Map<String, Object> vertexProps = new HashMap<>();
        vertexProps.put(DatabaseStructure.NodeProperty.VERTEX_TYPE.t(), MantaNodeLabel.NODE.toString());
        vertexProps.put(DatabaseStructure.NodeProperty.NODE_NAME.t(), name);
        vertexProps.put(DatabaseStructure.NodeProperty.NODE_TYPE.t(), type);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idParent", parentId);
        params.put("idResource", resourceId);
        params.put("vertexProps", vertexProps);
        params.put("edgeProps", edgeProps);

        Result result = tx.execute( String.format("MATCH (resource:%s), (parent:%s) WHERE ID(resource)=$idResource AND ID(parent)=$idParent" +
                        " CREATE (parent)<-[parentEdge:%s $edgeProps]-" +
                        "(node:%s $vertexProps)-[edge:%s $edgeProps]->(resource)" +
                        " RETURN node", MantaNodeLabel.RESOURCE.getVertexLabel(), MantaNodeLabel.NODE.getVertexLabel(),
                DatabaseStructure.MantaRelationshipType.HAS_PARENT.t(), MantaNodeLabel.NODE.getVertexLabel(), DatabaseStructure.MantaRelationshipType.HAS_RESOURCE.t()), params);
        return result.hasNext()? (Node)result.next().get("node"): null;
    }

    /**
     * Creates layer vertex
     * @param tx Database transaction
     * @param name Name of layer
     * @param type Type of layer
     * @return Created layer vertex
     */
    public Node createLayer(Transaction tx, String name, String type) {
        // Node properties
        Map<String, Object> vertexProps = new HashMap<>();
        vertexProps.put(DatabaseStructure.NodeProperty.VERTEX_TYPE.t(), MantaNodeLabel.LAYER.toString());
        vertexProps.put(DatabaseStructure.NodeProperty.LAYER_NAME.t(), name);
        vertexProps.put(DatabaseStructure.NodeProperty.LAYER_TYPE.t(), type);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("vertexProps", vertexProps);
        Result result = tx.execute( "CREATE (layer:Layer $vertexProps) RETURN layer", params);
        return result.hasNext()? (Node)result.next().get("layer"): null;
    }

    /**
     * Creates resource vertex linking to
     * @param tx Database transaction
     * @param root Super root node
     * @param name Resource name
     * @param type Resource type
     * @param description Resource description
     * @param layer Resource layer node
     * @param revisionInterval Revision interval
     * @return Created resource vertex
     */
    public Node createResource(Transaction tx, Node root, String name, String type, String description,
                                      Node layer, RevisionInterval revisionInterval) {
        Validate.notNull(root, "The argument 'root' was null.");
        Validate.notNull(layer, "The argument 'layer' was null.");
        Validate.notNull(revisionInterval, "The argument 'revisionInterval' was null.");

        // Root edge properties
        Map<String, Object> rootEdgeProps = new HashMap<>();
        rootEdgeProps.put(DatabaseStructure.EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        rootEdgeProps.put(DatabaseStructure.EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
        rootEdgeProps.put(DatabaseStructure.EdgeProperty.CHILD_NAME.t(), graphOperation.processValueForChildName(name));
        // Layer edge properties
        Map<String, Object> layerEdgeProps = new HashMap<>();
        layerEdgeProps.put(DatabaseStructure.EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        layerEdgeProps.put(DatabaseStructure.EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
        // Node properties
        Map<String, Object> vertexProps = new HashMap<>();
        vertexProps.put(DatabaseStructure.NodeProperty.VERTEX_TYPE.t(), MantaNodeLabel.RESOURCE.toString());
        vertexProps.put(DatabaseStructure.NodeProperty.RESOURCE_NAME.t(), name);
        vertexProps.put(DatabaseStructure.NodeProperty.RESOURCE_TYPE.t(), type);
        vertexProps.put(DatabaseStructure.NodeProperty.RESOURCE_DESCRIPTION.t(), description);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idRoot", root.getId());
        params.put("idLayer", layer.getId());
        params.put("vertexProps", vertexProps);
        params.put("rootEdgeProps", rootEdgeProps);
        params.put("layerEdgeProps", layerEdgeProps);

        Result result = tx.execute( String.format("MATCH (root), (layer) WHERE ID(root)=$idRoot AND ID(layer)=$idLayer" +
                " CREATE (root)<-[re:%s $rootEdgeProps]-(resource:%s $vertexProps)-" +
                "[le:%s $layerEdgeProps]->(layer) RETURN resource",
                DatabaseStructure.MantaRelationshipType.HAS_RESOURCE.t(), MantaNodeLabel.RESOURCE.getVertexLabel(), DatabaseStructure.MantaRelationshipType.IN_LAYER.t()),
                params);
        return result.hasNext()? (Node)result.next().get("resource"): null;
    }

    /**
     * Creates node attribute vertex
     * @param tx Database transaction
     * @param nodeId Source node id
     * @param key Key
     * @param value Value
     * @param revisionInterval Revision interval
     */
    public void createNodeAttribute(Transaction tx, Long nodeId, String key, Object value,
                                           RevisionInterval revisionInterval) {
        Validate.notNull(nodeId, "The argument 'nodeId' was null.");
        Validate.notNull(revisionInterval, "The argument 'revisionInterval' was null.");

        // Edge properties
        Map<String, Object> edgeProps = new HashMap<>();
        edgeProps.put(DatabaseStructure.EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        edgeProps.put(DatabaseStructure.EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
        // Node properties
        Map<String, Object> vertexProps = new HashMap<>();
        vertexProps.put(DatabaseStructure.NodeProperty.VERTEX_TYPE.t(), MantaNodeLabel.ATTRIBUTE.toString());
        vertexProps.put(DatabaseStructure.NodeProperty.ATTRIBUTE_NAME.t(), key);
        vertexProps.put(DatabaseStructure.NodeProperty.ATTRIBUTE_VALUE.t(), value);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idNode", nodeId);
        params.put("vertexProps", vertexProps);
        params.put("edgeProps", edgeProps);

        Result result = tx.execute(String.format("MATCH (node) WHERE ID(node)=$idNode" +
                " CREATE (node)-[edge:%s $edgeProps]->" +
                "(attribute:%s $vertexProps) return attribute", DatabaseStructure.MantaRelationshipType.HAS_ATTRIBUTE.t(), MantaNodeLabel.ATTRIBUTE.getVertexLabel()), params);
        Node n = result.hasNext()? (Node)result.next().get("attribute"): null;
    }

    /**
     * Creates new relationship between source and target vertex
     * Certain edge labels allow only specific type of source and target vertices
     * Certain edge labels require special properties
     * @param tx Database transaction
     * @param sourceVertexId Source vertex id
     * @param targetVertexId Target vertex id
     * @param label Edge label
     * @param properties Edge properties
     * @param interpolated Interpolated flag
     * @param revisionInterval Revision interval
     * @return Created revision
     */
    public Relationship createRelationship(Transaction tx, Long sourceVertexId, Long targetVertexId, DatabaseStructure.MantaRelationshipType label,
                                           Map<String, Object> properties, boolean interpolated, RevisionInterval revisionInterval) {
        Validate.notNull(sourceVertexId, "The argument 'source' was null.");
        Validate.notNull(targetVertexId, "The argument 'target' was null.");
        Validate.notNull(revisionInterval, "The argument 'revisionInterval' was null.");

        Map<String, Object> edgeProps = new HashMap<>(properties);
        edgeProps.put(DatabaseStructure.EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        edgeProps.put(DatabaseStructure.EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
        String sourceType = "";
        String targetType = "";
        switch (label) {
            case DIRECT:
            case FILTER:
            case HAS_PARENT:
            case MAPS_TO:
            case PERSPECTIVE:
                sourceType = targetType = MantaNodeLabel.NODE.getVertexLabel();
                break;
            case HAS_ATTRIBUTE:
                sourceType = MantaNodeLabel.NODE.getVertexLabel();
                targetType = MantaNodeLabel.ATTRIBUTE.getVertexLabel();
                break;
            case HAS_RESOURCE:
                sourceType = MantaNodeLabel.NODE.getVertexLabel();
                targetType = MantaNodeLabel.RESOURCE.getVertexLabel();
                break;
            case HAS_REVISION:
                sourceType = MantaNodeLabel.REVISION_ROOT.getVertexLabel();
                targetType = MantaNodeLabel.REVISION_NODE.getVertexLabel();
                break;
            case HAS_SOURCE:
                sourceType = MantaNodeLabel.SOURCE_ROOT.getVertexLabel();
                targetType = MantaNodeLabel.SOURCE_NODE.getVertexLabel();
                break;
            case IN_LAYER:
                sourceType = MantaNodeLabel.RESOURCE.getVertexLabel();
                targetType = MantaNodeLabel.LAYER.getVertexLabel();
                break;
            default:
                throw new IllegalStateException("Unknown label " + label + ".");
        }
        Map<String,Object> params = new HashMap<>();
        params.put("idSource", sourceVertexId);
        params.put("idTarget", targetVertexId);
        params.put("sourceLabel", sourceType);
        params.put("targetLabel", targetType);
        Result result;
        if (label == DatabaseStructure.MantaRelationshipType.HAS_RESOURCE || label == DatabaseStructure.MantaRelationshipType.HAS_PARENT) {
            params.put("edgeProps", edgeProps);
            params.put("edgeProperty", DatabaseStructure.EdgeProperty.CHILD_NAME.t());
            params.put("vertexType", DatabaseStructure.NodeProperty.VERTEX_TYPE.t());
            params.put("typeNode", MantaNodeLabel.NODE.toString());
            params.put("nameNode", DatabaseStructure.NodeProperty.NODE_NAME.t());
            params.put("nameResource", DatabaseStructure.NodeProperty.RESOURCE_NAME.t());
            if (label == DatabaseStructure.MantaRelationshipType.HAS_PARENT) {
                result = tx.execute(String.format("MATCH (source), (target) WHERE ID(source)=$idSource AND ID(target)=$idTarget" +
                        " AND $sourceLabel in labels(source) AND $targetLabel in labels(target)" +
                        " CREATE (source)-[edge:%s $edgeProps]->(target)" +
                        " SET edge.%s= CASE source[$vertexType]" +
                        " WHEN $typeNode THEN substring(source[$nameNode], 0, 200)" +
                        " ELSE substring(source[$nameResource], 0, 200) END " +
                        " RETURN edge", label.t(), DatabaseStructure.EdgeProperty.CHILD_NAME.t()), params);
            } else {
                params.put("targetLabel2", MantaNodeLabel.SUPER_ROOT.getVertexLabel());
                result = tx.execute(String.format("MATCH (source), (target) WHERE ID(source)=$idSource AND ID(target)=$idTarget" +
                        " AND (" +
                        "($sourceLabel in labels(source) AND $targetLabel in labels(target))" +
                        " OR ($targetLabel in labels(source) AND $targetLabel2 in labels(target))" +
                        ")" +
                        " CREATE (source)-[edge:%s $edgeProps]->(target)" +
                        " SET edge.%s= CASE source[$vertexType]" +
                        " WHEN $typeNode THEN substring(source[$nameNode], 0, 200)" +
                        " ELSE substring(source[$nameResource], 0, 200) END " +
                        " RETURN edge", label.t(), DatabaseStructure.EdgeProperty.CHILD_NAME.t()), params);
            }
        } else {
            if (label == DatabaseStructure.MantaRelationshipType.HAS_SOURCE) {
                Node targetNode = graphOperation.getNodeById(tx, targetVertexId);
                String localName = graphOperation.processValueForLocalName(
                        targetNode.getProperty(DatabaseStructure.NodeProperty.SOURCE_NODE_LOCAL.t()).toString());
                edgeProps.put(DatabaseStructure.EdgeProperty.SOURCE_LOCAL_NAME.t(), localName);
            } else if (label == DatabaseStructure.MantaRelationshipType.DIRECT || label == DatabaseStructure.MantaRelationshipType.FILTER
                    || label == DatabaseStructure.MantaRelationshipType.MAPS_TO || label == DatabaseStructure.MantaRelationshipType.PERSPECTIVE) {
                edgeProps.put(DatabaseStructure.EdgeProperty.TARGET_ID.t(), targetVertexId);
                edgeProps.put(DatabaseStructure.EdgeProperty.INTERPOLATED.t(), interpolated);
            }
            params.put("edgeProps", edgeProps);
            result = tx.execute(String.format("MATCH (source), (target) WHERE ID(source)=$idSource AND ID(target)=$idTarget" +
                    " AND $sourceLabel in labels(source) AND $targetLabel in labels(target)" +
                    " CREATE (source)-[edge:%s $edgeProps]->(target)" +
                    " RETURN edge", label.t()), params);
        }
        return result.hasNext()? (Relationship) result.next().get("edge"): null;
    }

    /**
     * Copies given relationship and adds new properties to new relationship
     * @param tx Database properties
     * @param relationship Given relationship
     * @param newProperties New properties
     * @param revisionInterval Revision interval.
     * @return New relationship
     */
    public Relationship copyRelationshipWithNewProperties(Transaction tx,
                                                          Relationship relationship,
                                                          Map<String, Object> newProperties,
                                                          RevisionInterval revisionInterval) {
        Validate.notNull(tx, "The argument 'tx' was null.");
        Validate.notNull(relationship, "The argument 'relationship' was null.");
        Validate.notNull(revisionInterval, "The argument 'revisionInterval' was null.");

        Map<String, Object> props = new HashMap<>(newProperties);
        props.put(DatabaseStructure.EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        props.put(DatabaseStructure.EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());

        Map<String,Object> params = new HashMap<>();
        params.put("props", props );
        params.put("idSource", relationship.getStartNodeId());
        params.put("idTarget", relationship.getEndNodeId());

        Result result = tx.execute( String.format("MATCH (n),(m)" +
                " WHERE id(n) = $idSource AND id(m) = $idTarget" +
                " CREATE (n)-[r:%s $props]->(m)" +
                " RETURN r", relationship.getType()), params);
        return result.hasNext()? (Relationship) result.next().get("r"): null;
    }

    /**
     * Adds relationship property to given relationship
     * @param tx Database transaction
     * @param relationship Given relationship
     * @param propertyName Name of the property
     * @param propertyValue Value of the property
     */
    public void addRelationshipProperty(Transaction tx, Relationship relationship,
                                        String propertyName, Object propertyValue) {
        // Edge properties
        Map<String, Object> edgeProps = new HashMap<>();
        edgeProps.put(propertyName, propertyValue);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idRelation", relationship.getId());
        params.put("edgeProps", edgeProps);
        tx.execute( "MATCH ()-[r]-() WHERE ID(r)=$idRelation SET r += $edgeProps RETURN r", params);
    }

    /**
     * Adds relationship property to given relationship
     * @param tx Database transaction
     * @param nodeId Id of node
     * @param nodeProps Property map
     */
    public void addNodeProperty(Transaction tx, Long nodeId, Map<String, Object> nodeProps) {
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idNode", nodeId);
        params.put("nodeProps", nodeProps);
        tx.execute( "MATCH (v) WHERE ID(v)=$idNode SET v += $nodeProps RETURN v", params);
    }
}