package eu.profinit.manta.graphplayground.repository.merger.connector;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.EdgeProperty;
import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.MantaRelationshipType;
import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.NodeProperty;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerType;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.MergerOutput;
import org.apache.commons.lang3.Validate;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.function.Function;

import static org.neo4j.driver.Values.parameters;

/**
 * Class containing methods for working with graph objects.
 * Contains methods for creating and modifying graph objects.
 *
 * @author tpolacok
 */
@Repository
public class GraphCreation {

    /**
     * Graph operation instance.
     */
    private final GraphOperation graphOperation;

    /**
     * @param graphOperation Graph operation instance.
     */
    public GraphCreation(GraphOperation graphOperation) {
        this.graphOperation = graphOperation;
    }

    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphCreation.class);

    /**
     * Stored parallel merge.
     * @param tx Transaction object.
     * @param mergeObjects List of lists (different types) of merge objects
     * @param revision Merged revision.
     * @param threads Number of threads.
     * @return Merging result. {@link MergerOutput}
     */
    public static MergerOutput storedParallelMerge(Transaction tx, List<List<List<String>>> mergeObjects,
                                                   Double revision, int threads,
                                                   MergerType type) {
        if (threads == 1)
            return storedSequentialMerge(tx, mergeObjects, revision);
        Result result = tx.run(
                "CALL manta.mergeParallel($mergeObjects, $revision, $threads, $type) \n" +
                        "YIELD time, newObjects, errorObjects, existedObjects, \n" +
                        " unknownTypes, processedObjects, requestedSourceCodes \n" +
                        "RETURN time, newObjects, errorObjects, existedObjects, \n" +
                        " unknownTypes, processedObjects, requestedSourceCodes",
                parameters("mergeObjects", mergeObjects,
                        "revision", revision,
                        "threads", threads,
                        "type", type.toString()));
        Record record = result.single();
        Function<Value, Integer> mapVal2Int = Value::asInt;
        return new MergerOutput(record.get("time").asLong(), record.get("newObjects").asMap(mapVal2Int),
                record.get("errorObjects").asMap(mapVal2Int), record.get("existedObjects").asMap(mapVal2Int),
                record.get("unknownTypes").asLong(), record.get("processedObjects").asLong(),
                record.get("requestedSourceCodes").asMap(Value::asString));
    }

    /**
     * Stored sequential merge.
     * @param tx Transaction object.
     * @param mergeObjects List of lists (different types) of merge objects
     * @param revision Merged revision.
     * @return Merging result. {@link MergerOutput}
     */
    public static MergerOutput storedSequentialMerge(Transaction tx, List<List<List<String>>> mergeObjects,
                                                     Double revision) {
        Result result = tx.run(
                "CALL manta.mergeSequential($mergeObjects, $revision) \n" +
                        "YIELD time, newObjects, errorObjects, existedObjects, \n" +
                        " unknownTypes, processedObjects, requestedSourceCodes \n" +
                        "RETURN time, newObjects, errorObjects, existedObjects, \n" +
                        " unknownTypes, processedObjects, requestedSourceCodes",
                parameters("mergeObjects", mergeObjects,
                        "revision", revision));
        Record record = result.single();
        Function<Value, Integer> mapVal2Int = Value::asInt;
        return new MergerOutput(record.get("time").asLong(), record.get("newObjects").asMap(mapVal2Int),
                record.get("errorObjects").asMap(mapVal2Int), record.get("existedObjects").asMap(mapVal2Int),
                record.get("unknownTypes").asLong(), record.get("processedObjects").asLong(),
                record.get("requestedSourceCodes").asMap(Value::asString));
    }

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
        edgeProps.put(EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        edgeProps.put(EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
        edgeProps.put(EdgeProperty.SOURCE_LOCAL_NAME.t(), graphOperation.processValueForLocalName(scLocalName));
        // Node properties
        Map<String, Object> vertexProps = new HashMap<>();
        vertexProps.put(NodeProperty.VERTEX_TYPE.t(), MantaNodeLabel.SOURCE_NODE.toString());
        vertexProps.put(NodeProperty.SOURCE_NODE_ID.t(), id);
        vertexProps.put(NodeProperty.SOURCE_NODE_LOCAL.t(), scLocalName);
        vertexProps.put(NodeProperty.SOURCE_NODE_HASH.t(), scHash);
        vertexProps.put(NodeProperty.SOURCE_NODE_TECHNOLOGY.t(), scTechnology);
        vertexProps.put(NodeProperty.SOURCE_NODE_CONNECTION.t(), scConnection);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idSource", sourceRoot.id());
        params.put("edgeProps", edgeProps);
        params.put("vertexProps", vertexProps);
        return tx.run(String.format("MATCH (source) WHERE ID(source)=$idSource" +
                " CREATE (source)-[edge:%s $edgeProps]->" +
                "(scNode:%s $vertexProps)" +
                " RETURN scNode", MantaRelationshipType.HAS_SOURCE.t(), MantaNodeLabel.SOURCE_NODE.getVertexLabel()), params)
                .single()
                .get("scNode")
                .asNode();
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
        edgeProps.put(EdgeProperty.TRAN_START.t(), startRevision);
        edgeProps.put(EdgeProperty.TRAN_END.t(), endRevision);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("edgeProps", edgeProps);
        params.put("idSource", source.id());
        params.put("idTarget", target.id());
        tx.run( String.format("MATCH (source), (target) WHERE ID(source)=$idSource AND ID(target)=$idTarget" +
                " CREATE (source)-[edge:%s $edgeProps]->(target)", label), params);
    }

    /**
     * Creates node linked to single parent/resource based on the label
     * @param tx Database transaction
     * @param parentId Id of parent node
     * @param label Edge label {@link MantaRelationshipType#HAS_PARENT} or {@link MantaRelationshipType#HAS_RESOURCE}
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
        edgeProps.put(EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        edgeProps.put(EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
        edgeProps.put(EdgeProperty.CHILD_NAME.t(), graphOperation.processValueForChildName(name));
        // Node properties
        Map<String, Object> vertexProps = new HashMap<>();
        vertexProps.put(NodeProperty.VERTEX_TYPE.t(), MantaNodeLabel.NODE.toString());
        vertexProps.put(NodeProperty.NODE_NAME.t(), name);
        vertexProps.put(NodeProperty.NODE_TYPE.t(), type);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idParent", parentId);
        params.put("vertexProps", vertexProps);
        params.put("edgeProps", edgeProps);
        return tx.run( String.format("MATCH (parent) WHERE ID(parent)=$idParent" +
                " CREATE (node:%s $vertexProps)-[edge:%s $edgeProps]->(parent)" +
                " RETURN node", MantaNodeLabel.NODE.getVertexLabel(), label), params)
                .single().get("node").asNode();
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
        edgeProps.put(EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        edgeProps.put(EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
        edgeProps.put(EdgeProperty.CHILD_NAME.t(), graphOperation.processValueForChildName(name));
        // Node properties
        Map<String, Object> vertexProps = new HashMap<>();
        vertexProps.put(NodeProperty.VERTEX_TYPE.t(), MantaNodeLabel.NODE.toString());
        vertexProps.put(NodeProperty.NODE_NAME.t(), name);
        vertexProps.put(NodeProperty.NODE_TYPE.t(), type);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idParent", parentId);
        params.put("idResource", resourceId);
        params.put("vertexProps", vertexProps);
        params.put("edgeProps", edgeProps);

        return tx.run( String.format("MATCH (resource:%s), (parent:%s) WHERE ID(resource)=$idResource AND ID(parent)=$idParent" +
                        " CREATE (parent)<-[parentEdge:%s $edgeProps]-" +
                        "(node:%s $vertexProps)-[edge:%s $edgeProps]->(resource)" +
                        " RETURN node", MantaNodeLabel.RESOURCE.getVertexLabel(), MantaNodeLabel.NODE.getVertexLabel(),
                MantaRelationshipType.HAS_PARENT.t(), MantaNodeLabel.NODE.getVertexLabel(), MantaRelationshipType.HAS_RESOURCE.t()), params)
                .single().get("node").asNode();
    }

    /**
     * Vytvoří uzel pro revizi.
     * @param tx transkace pro přístup k db
     * @param revisionRoot revision root
     * @param revisionNumber číslo nově vytvářené revize
     * @param isCommitted true, jestliže je nově zakládaná revize již commitnutá
     * @param time čas commitnutí nově zakládané revize
     * @return vrchol odpovídající nové revizi
     */
    /**
     * Creates revision  node
     * @param tx Database transaction
     * @param revisionRoot Revision root node
     * @param revisionNumber New revision number
     * @param previousRevisionNode Previous revision node
     * @param nextRevisionNode Next revision node
     * @param isCommitted Committed tag
     * @param time Time of creation
     * @return Created revision node vertex
     */
    public Node createRevisionNode(Transaction tx, Node revisionRoot, Double revisionNumber,
                                   Node previousRevisionNode, Node nextRevisionNode, Boolean isCommitted, Date time) {
        Validate.notNull(revisionRoot, "The argument 'revisionRoot' was null.");
        Validate.notNull(revisionNumber, "The argument 'revisionNumber' was null.");
        double previousRevision = -1.0;
        double nextRevision = -1.0;
        long previousRevisionId = -1;
        long nextRevisionId = -1;

        if (previousRevisionNode != null) {
            previousRevision = previousRevisionNode.get(NodeProperty.REVISION_NODE_REVISION.t()).asDouble();
            previousRevisionId = previousRevisionNode.id();
        }

        if (nextRevisionNode != null) {
            nextRevision = nextRevisionNode.get(NodeProperty.REVISION_NODE_REVISION.t()).asDouble();
            nextRevisionId = nextRevisionNode.id();
        }

        String rootProperty = isCommitted ? NodeProperty.LATEST_COMMITTED_REVISION.t()
                : NodeProperty.LATEST_UNCOMMITTED_REVISION.t();

        // Edge properties
        Map<String, Object> edgeProps = new HashMap<>();
        edgeProps.put(EdgeProperty.TRAN_START.t(), revisionNumber);
        edgeProps.put(EdgeProperty.TRAN_END.t(), revisionNumber);
        // Node properties
        Map<String, Object> vertexProps = new HashMap<>();
        vertexProps.put(NodeProperty.VERTEX_TYPE.t(), MantaNodeLabel.REVISION_NODE.toString());
        vertexProps.put(NodeProperty.REVISION_NODE_REVISION.t(), revisionNumber);
        vertexProps.put(NodeProperty.REVISION_NODE_COMMITTED.t(), isCommitted);
        vertexProps.put(NodeProperty.REVISION_NODE_COMMIT_TIME.t(), time.toString());
        vertexProps.put(NodeProperty.REVISION_NODE_PREVIOUS_REVISION.t(), previousRevision);
        vertexProps.put(NodeProperty.REVISION_NODE_NEXT_REVISION.t(), nextRevision);
        // Previous revision properties
        Map<String, Object> previousRevisionProps = new HashMap<>();
        previousRevisionProps.put(NodeProperty.REVISION_NODE_NEXT_REVISION.t(), revisionNumber);
        // Next revision properties
        Map<String, Object> nextRevisionProps = new HashMap<>();
        nextRevisionProps.put(NodeProperty.REVISION_NODE_PREVIOUS_REVISION.t(), revisionNumber);
        // Revision root properties
        Map<String, Object> revisionRootProps = new HashMap<>();
        revisionRootProps.put(rootProperty, revisionNumber);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idRevisionRoot", revisionRoot.id());
        params.put("idNextRevision", nextRevisionId);
        params.put("idPreviousRevision", previousRevisionId);
        params.put("vertexProps", vertexProps);
        params.put("edgeProps", edgeProps);
        params.put("previousRevisionProps", previousRevisionProps);
        params.put("nextRevisionProps", nextRevisionProps);
        params.put("revisionRootProps", revisionRootProps);
        Result revision = tx.run( String.format(" MATCH (revisionRoot) WHERE id(revisionRoot)=$idRevisionRoot" +
                        " OPTIONAL MATCH(previousRevision) WHERE id(previousRevision)=$idPreviousRevision" +
                        " OPTIONAL MATCH(nextRevision) WHERE id(nextRevision)=$idNextRevision" +
                        " SET previousRevision += $previousRevisionProps" +
                        " SET nextRevision += $nextRevisionProps" +
                        " SET revisionRoot += $revisionRootProps " +
                        " CREATE (revisionRoot)-[:%s $edgeProps]" +
                        "->(revision:%s $vertexProps) RETURN revision",
                MantaRelationshipType.HAS_REVISION.t(), MantaNodeLabel.REVISION_NODE.getVertexLabel()), params);

        return revision.hasNext()? revision.single().get("revision").asNode(): null;
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
        vertexProps.put(NodeProperty.VERTEX_TYPE.t(), MantaNodeLabel.LAYER.toString());
        vertexProps.put(NodeProperty.LAYER_NAME.t(), name);
        vertexProps.put(NodeProperty.LAYER_TYPE.t(), type);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("vertexProps", vertexProps);
        Result layer = tx.run( "CREATE (layer:Layer $vertexProps) RETURN layer", params);
        return layer.hasNext() ? layer.single().get("layer").asNode(): null;
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
        rootEdgeProps.put(EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        rootEdgeProps.put(EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
        rootEdgeProps.put(EdgeProperty.CHILD_NAME.t(), graphOperation.processValueForChildName(name));
        // Layer edge properties
        Map<String, Object> layerEdgeProps = new HashMap<>();
        layerEdgeProps.put(EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        layerEdgeProps.put(EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
        // Node properties
        Map<String, Object> vertexProps = new HashMap<>();
        vertexProps.put(NodeProperty.VERTEX_TYPE.t(), MantaNodeLabel.RESOURCE.toString());
        vertexProps.put(NodeProperty.RESOURCE_NAME.t(), name);
        vertexProps.put(NodeProperty.RESOURCE_TYPE.t(), type);
        vertexProps.put(NodeProperty.RESOURCE_DESCRIPTION.t(), description);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idRoot", root.id());
        params.put("idLayer", layer.id());
        params.put("vertexProps", vertexProps);
        params.put("rootEdgeProps", rootEdgeProps);
        params.put("layerEdgeProps", layerEdgeProps);

        Result resource = tx.run( String.format("MATCH (root), (layer) WHERE ID(root)=$idRoot AND ID(layer)=$idLayer" +
                        " CREATE (root)<-[re:%s $rootEdgeProps]-(resource:%s $vertexProps)-" +
                        "[le:%s $layerEdgeProps]->(layer) RETURN resource",
                MantaRelationshipType.HAS_RESOURCE.t(), MantaNodeLabel.RESOURCE.getVertexLabel(), MantaRelationshipType.IN_LAYER.t()),
                params);
        return resource.hasNext() ? resource.single().get("resource").asNode(): null;
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
        edgeProps.put(EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        edgeProps.put(EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
        // Node properties
        Map<String, Object> vertexProps = new HashMap<>();
        vertexProps.put(NodeProperty.VERTEX_TYPE.t(), MantaNodeLabel.ATTRIBUTE.toString());
        vertexProps.put(NodeProperty.ATTRIBUTE_NAME.t(), key);
        vertexProps.put(NodeProperty.ATTRIBUTE_VALUE.t(), value);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idNode", nodeId);
        params.put("vertexProps", vertexProps);
        params.put("edgeProps", edgeProps);

        Result attribute = tx.run(String.format("MATCH (node) WHERE ID(node)=$idNode" +
                " CREATE (node)-[edge:%s $edgeProps]->" +
                "(attribute:%s $vertexProps)", MantaRelationshipType.HAS_ATTRIBUTE.t(), MantaNodeLabel.ATTRIBUTE.getVertexLabel()), params);
    }

    /**
     * Creates new relationship between source and target vertex
     * Certain edge labels allow only specific type of source and target vertices
     * Certain edge labels require special properties
     * @param tx Database transaction
     * @param sourceVertexId Source vertex id
     * @param targetVertexId Target vertex id
     * @param label Edge label
     * @param interpolated Interpolated flag
     * @param revisionInterval Revision interval
     * @return Created revision
     */
    public Relationship createRelationship(Transaction tx, Long sourceVertexId, Long targetVertexId, MantaRelationshipType label,
                                           boolean interpolated, RevisionInterval revisionInterval) {
        Validate.notNull(sourceVertexId, "The argument 'source' was null.");
        Validate.notNull(targetVertexId, "The argument 'target' was null.");
        Validate.notNull(revisionInterval, "The argument 'revisionInterval' was null.");

        Map<String, Object> edgeProps = new HashMap<>();
        edgeProps.put(EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        edgeProps.put(EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
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
        Result relationship;
        if (label == MantaRelationshipType.HAS_RESOURCE || label == MantaRelationshipType.HAS_PARENT) {
            params.put("edgeProps", edgeProps);
            params.put("edgeProperty", EdgeProperty.CHILD_NAME.t());
            params.put("vertexType", NodeProperty.VERTEX_TYPE.t());
            params.put("typeNode", MantaNodeLabel.NODE.toString());
            params.put("nameNode", NodeProperty.NODE_NAME.t());
            params.put("nameResource", NodeProperty.RESOURCE_NAME.t());
            if (label == MantaRelationshipType.HAS_PARENT) {
                relationship = tx.run(String.format("MATCH (source), (target) WHERE ID(source)=$idSource AND ID(target)=$idTarget" +
                        " AND $sourceLabel in labels(source) AND $targetLabel in labels(target)" +
                        " CREATE (source)-[edge:%s $edgeProps]->(target)" +
                        " SET edge.%s= CASE source[$vertexType]" +
                        " WHEN $typeNode THEN substring(source[$nameNode], 0, 200)" +
                        " ELSE substring(source[$nameResource], 0, 200) END " +
                        " RETURN edge", label.t(), EdgeProperty.CHILD_NAME.t()), params);
            } else {
                params.put("targetLabel2", MantaNodeLabel.SUPER_ROOT.getVertexLabel());
                relationship = tx.run(String.format("MATCH (source), (target) WHERE ID(source)=$idSource AND ID(target)=$idTarget" +
                        " AND (" +
                        "($sourceLabel in labels(source) AND $targetLabel in labels(target))" +
                        " OR ($targetLabel in labels(source) AND $targetLabel2 in labels(target))" +
                        ")" +
                        " CREATE (source)-[edge:%s $edgeProps]->(target)" +
                        " SET edge.%s= CASE source[$vertexType]" +
                        " WHEN $typeNode THEN substring(source[$nameNode], 0, 200)" +
                        " ELSE substring(source[$nameResource], 0, 200) END " +
                        " RETURN edge", label.t(), EdgeProperty.CHILD_NAME.t()), params);
            }
        } else {
            if (label == MantaRelationshipType.HAS_SOURCE) {
                Node targetNode = graphOperation.getNodeById(tx, targetVertexId);
                String localName = graphOperation.processValueForLocalName(targetNode.get(NodeProperty.SOURCE_NODE_LOCAL.t()).asString());
                edgeProps.put(EdgeProperty.SOURCE_LOCAL_NAME.t(), localName);
            } else if (label == MantaRelationshipType.DIRECT || label == MantaRelationshipType.FILTER
                    || label == MantaRelationshipType.MAPS_TO || label == MantaRelationshipType.PERSPECTIVE) {
                edgeProps.put(EdgeProperty.TARGET_ID.t(), targetVertexId);
                edgeProps.put(EdgeProperty.INTERPOLATED.t(), interpolated);
            }
            params.put("edgeProps", edgeProps);
            relationship = tx.run(String.format("MATCH (source), (target) WHERE ID(source)=$idSource AND ID(target)=$idTarget" +
                    " AND $sourceLabel in labels(source) AND $targetLabel in labels(target)" +
                    " CREATE (source)-[edge:%s $edgeProps]->(target)" +
                    " RETURN edge", label.t()), params);
        }
        return relationship.hasNext() ? relationship.single().get("edge").asRelationship() : null;
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

        Map<String, Object> props = new HashMap<>(relationship.asMap());
        props.remove(EdgeProperty.TARGET_ID.t());
        props.put(EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
        props.put(EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
        props.putAll(newProperties);

        Map<String,Object> params = new HashMap<>();
        params.put("props", props );
        params.put("idSource", relationship.startNodeId());
        params.put("idTarget", relationship.endNodeId());

        Result newRelationship = tx.run( String.format("MATCH (n),(m)" +
                " WHERE id(n) = $idSource AND id(m) = $idTarget" +
                " CREATE (n)-[r:%s $props]->(m)" +
                " RETURN r", relationship.type()), params);
        return newRelationship.hasNext()? newRelationship.single().get("r").asRelationship(): null;
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
        params.put("idRelation", relationship.id());
        params.put("edgeProps", edgeProps);
        tx.run( "MATCH ()-[r]-() WHERE ID(r)=$idRelation SET r += $edgeProps RETURN r", params);
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
        tx.run( "MATCH (v) WHERE ID(v)=$idNode SET v += $nodeProps RETURN v", params);
    }
}