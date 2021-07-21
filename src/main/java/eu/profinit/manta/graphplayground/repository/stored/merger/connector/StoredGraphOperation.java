package eu.profinit.manta.graphplayground.repository.stored.merger.connector;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.EdgeProperty;
import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.MantaRelationshipType;
import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.NodeProperty;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import eu.profinit.manta.graphplayground.model.manta.core.EdgeIdentification;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.model.manta.util.StoredMantaNodeLabelConverter;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Class containing methods for working with graph objects.
 * Contains methods for traversing and retrieval operations.
 *
 * @author tpolacok
 */
@Repository
public class StoredGraphOperation {

    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(StoredGraphOperation.class);

    /**
     * Decimal format with 6 decimal digits for printing
     */
    private static NumberFormat formatter = new DecimalFormat("#0.000000");

    /**
     * Number of parts of qualified name
     */
    private static final int QUALIFIED_NAME_PARTS = 3;

    /**s
     * Revision utils instance.
     */
    private final StoredRevisionUtils revisionUtils;

    /**
     * @param revisionUtils Revision utils instance.
     */
    public StoredGraphOperation(StoredRevisionUtils revisionUtils) {
        this.revisionUtils = revisionUtils;
    }

    /**
     * Returns type of vertex by its type.
     *
     * @param vertex Vertex for which type is returned.
     * @return Type of vertex.
     */
    public String getType(Node vertex) {
        if (vertex == null) {
            throw new IllegalArgumentException("The argument vertex must not be null.");
        }

        MantaNodeLabel type = StoredMantaNodeLabelConverter.getType(vertex);
        switch (type) {
            case LAYER:
                return vertex.getProperty(NodeProperty.LAYER_TYPE.t()).toString();
            case NODE:
                return vertex.getProperty(NodeProperty.NODE_TYPE.t()).toString();
            case RESOURCE:
                return vertex.getProperty(NodeProperty.RESOURCE_TYPE.t()).toString();
            case ATTRIBUTE:
                return vertex.getProperty(NodeProperty.ATTRIBUTE_NAME.t()).toString();
            case SUPER_ROOT:
                return MantaNodeLabel.SUPER_ROOT.toString();
            case REVISION_ROOT:
                return MantaNodeLabel.REVISION_ROOT.toString();
            case REVISION_NODE:
                return MantaNodeLabel.REVISION_NODE.toString();
            case SOURCE_ROOT:
                return MantaNodeLabel.SOURCE_ROOT.toString();
            case SOURCE_NODE:
                return MantaNodeLabel.SOURCE_NODE.toString();
            default:
                throw new IllegalStateException("Unknown type of vertex " + type + ".");
        }
    }

    /**
     * Returns name of the vertex by its type.
     *
     * @param vertex Vertex for which name is retrieved
     * @return Name of the vertex.
     * @throws IllegalArgumentException Vertex is null
     * @throws IllegalStateException    Unknown vertex type
     */
    public String getName(Node vertex) {
        if (vertex == null) {
            throw new IllegalArgumentException("The argument vertex was null.");
        }
        MantaNodeLabel type = StoredMantaNodeLabelConverter.getType(vertex);
        switch (type) {
            case LAYER:
                return vertex.getProperty(NodeProperty.LAYER_NAME.t()).toString();
            case NODE:
                return vertex.getProperty(NodeProperty.NODE_NAME.t()).toString();
            case RESOURCE:
                return vertex.getProperty(NodeProperty.RESOURCE_NAME.t()).toString();
            case ATTRIBUTE:
                return vertex.getProperty(NodeProperty.ATTRIBUTE_NAME.t()).toString();
            case SUPER_ROOT:
                return MantaNodeLabel.SUPER_ROOT.toString();
            case REVISION_ROOT:
                return MantaNodeLabel.REVISION_ROOT.toString();
            case REVISION_NODE:
                return "revision[" + formatter.format(vertex.getProperty(NodeProperty.REVISION_NODE_REVISION.t()).toString()) + "]";
            case SOURCE_ROOT:
                return MantaNodeLabel.SOURCE_ROOT.toString();
            case SOURCE_NODE:
                return vertex.getProperty(NodeProperty.SOURCE_NODE_LOCAL.t()).toString();
            default:
                throw new IllegalStateException("Unknown type of vertex " + type + ".");
        }
    }

    /**
     * Fetches resource of given node
     * @param tx Database transaction
     * @param nodeId Node id
     * @return Resource of given node
     */
    public Node getResource(Transaction tx, Long nodeId) {
        Validate.notNull(nodeId, "The argument 'nodeId' was null.");
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idNode", nodeId);
        params.put("resourceLabel", MantaNodeLabel.RESOURCE.getVertexLabel());
        Result result = tx.execute(String.format("MATCH path=(node)<-[:%s*0..1]-" +
                        "()-[:%s*0..]->()-[:%s]->(resource)" +
                        " WHERE ID(node)=$idNode " +
                        " RETURN CASE WHEN $resourceLabel in labels(node)" +
                        " THEN node ELSE resource END as resource ORDER BY LENGTH(path) ASC LIMIT 1",
                MantaRelationshipType.HAS_ATTRIBUTE.t(), MantaRelationshipType.HAS_PARENT.t(), MantaRelationshipType.HAS_RESOURCE.t()),
                params);
        if (result.hasNext()) {
            return (Node)result.next().get("resource");
        } else {
            LOGGER.warn("Vertex with id: {} does not have resource.", nodeId);
            throw new IllegalStateException("Vertex with id " + nodeId + " does not have resource.");
        }
    }

    /**
     * Fetches layer of given node
     * @param tx Database transaction
     * @param nodeId Node id
     * @return Layer of given node
     */
    public Node getLayer(Transaction tx, Long nodeId) {
        Validate.notNull(nodeId, "The argument 'nodeId' was null.");
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idNode", nodeId);
        params.put("layerLabel", MantaNodeLabel.LAYER.getVertexLabel());
        params.put("resourceLabel", MantaNodeLabel.RESOURCE.getVertexLabel());
        Result result = tx.execute(String.format("MATCH (node) WHERE ID(node)=$idNode" +
                        " WITH node" +
                        " OPTIONAL MATCH (node)<-[:%s*0..1]-()-[:%s*0..]->()-[:%s]->(resource)-[:%s]->(layer)" +
                        " WITH node, layer" +
                        " OPTIONAL MATCH (node)-[:inLayer]->(layer2)" +
                        " RETURN CASE WHEN $layerLabel in labels(node) THEN node" +
                        " WHEN $resourceLabel in labels(node) THEN layer2" +
                        " ELSE layer" +
                        " END as layer LIMIT 1",
                MantaRelationshipType.HAS_ATTRIBUTE.t(), MantaRelationshipType.HAS_PARENT.t(),
                MantaRelationshipType.HAS_RESOURCE.t(), MantaRelationshipType.IN_LAYER.t()),
                params);
        if (result.hasNext()) {
            return (Node)result.next().get("layer");
        }
        throw new IllegalStateException("Vertex with id:'" + nodeId + "' is not in any layer.");
    }

    /**
     * Fetches relationship by its identification
     * @param tx Database transaction
     * @param edgeIdentification Identification of the edge
     * @return Fetched relationship
     */
    public Relationship getRelationShip(Transaction tx, EdgeIdentification edgeIdentification) {
        Validate.notNull(tx, "The argument 'tx' was null.");
        Validate.notNull(edgeIdentification, "The argument 'edgeIdentification' was null.");
        Result result = tx.execute("MATCH ()-[r]->() WHERE ID(r)=$idRelation RETURN r",
                Map.of("idRelation", edgeIdentification.getRelationId()));
        return result.hasNext()? (Relationship) result.next().get("r"): null;
    }

    public Relationship getRelationShip(Transaction tx, EdgeIdentification edgeIdentification, RevisionInterval
            revisionInterval) {
        Validate.notNull(tx, "The argument 'tx' was null.");
        Validate.notNull(edgeIdentification, "The argument 'edgeIdentification' was null.");
        Validate.notNull(revisionInterval, "The argument 'revisionInterval' was null.");
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idSource", edgeIdentification.getStartId());
        params.put("idTarget", edgeIdentification.getEndId());
        params.put("transStart", EdgeProperty.TRAN_START.t());
        params.put("transStartVal", revisionInterval.getEnd());
        params.put("transEnd", EdgeProperty.TRAN_END.t());
        params.put("transEndVal", revisionInterval.getStart());

        Result result = tx.execute(String.format("MATCH (source)-[r:%s]-(target)" +
                " WHERE r[$transStart] <= $transStartVal AND r[$transEnd] >= $transEndVal" +
                " AND ID(source) = $idSource AND ID(target) = $idTarget" +
                " RETURN r", edgeIdentification.getLabel().t()), params);
        return result.hasNext()? (Relationship) result.next().get("r"): null;
    }

    /**
     * Fetches relationships of given label between given vertices belonging to given revision
     * @param tx Database transaction
     * @param sourceId Source vertex id
     * @param targetId Target vertex id
     * @param label Label of the edge
     * @param latestCommittedRevision Latest committed revision
     * @return List of relationships
     */
    public List<Relationship> getRelationships(Transaction tx, Long sourceId, Long targetId,
                                               String label, Double latestCommittedRevision) {
        Validate.notNull(sourceId, "The argument 'sourceId' was null.");
        Validate.notNull(targetId, "The argument 'targetId' was null.");
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idSource", sourceId);
        params.put("idTarget", targetId);
        params.put("transEnd", EdgeProperty.TRAN_END.t());
        params.put("transEndVal", latestCommittedRevision);

        params.put("edgeTarget", EdgeProperty.TARGET_ID.t());
        params.put("edgeTargetValue", targetId);

        Result result = tx.execute("MATCH (source)-[r:" + label + "]->(target)" +
                " WHERE r[$transEnd] >= $transEndVal AND ID(target) = $idTarget AND ID(source) = $idSource" +
                " AND r[$edgeTarget]=$edgeTargetValue" +
                " RETURN r", params);
        if (result.hasNext()) {
            return result.stream()
                    .map(entry -> (Relationship)entry.get("r"))
                    .collect(Collectors.toList());
        } else return Collections.emptyList();
    }

    /**
     * Fetches outgoing relationships of given label from given node
     * @param tx Database transaction
     * @param sourceId Source vertex id
     * @param label Edge label
     * @param latestCommittedRevision Latest committed revision
     * @return List of relationships
     */
    public List<Relationship> getOutgoingRelationships(Transaction tx, Long sourceId,
                                                       String label, Double latestCommittedRevision) {
        Validate.notNull(sourceId, "The argument 'sourceId' was null.");
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idSource", sourceId);
        params.put("transEnd", EdgeProperty.TRAN_END.t());
        params.put("transEndVal", latestCommittedRevision);
        Result result = tx.execute(String.format("MATCH (source)-[r:%s]->(target)" +
                " WHERE r[$transEnd] >= $transEndVal AND ID(source) = $idSource" +
                " RETURN r", label), params);
        return result.hasNext()
                ? result.stream()
                .map(entry -> (Relationship)entry.get("r"))
                .collect(Collectors.toList())
                : Collections.emptyList();
    }

    /**
     * Fetches children nodes of given node
     * @param tx Database transaction
     * @param parentId Parent vertex id
     * @param name Name of child
     * @param type Type of child
     * @param label Label of control edge
     * @param nameProperty Name property
     * @param typeProperty Type property
     * @param latestCommittedRevision Latest committed revision
     * @return List of nodes
     */
    public List<Node> getChildren(Transaction tx, Long parentId, String name, String type, String label,
                                  String nameProperty, String typeProperty, Double latestCommittedRevision) {
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idParent", parentId);
        params.put("transEnd", EdgeProperty.TRAN_END.t());
        params.put("transEndVal", latestCommittedRevision);

        params.put("edgeChild", EdgeProperty.CHILD_NAME.t());
        params.put("edgeChildValue", processValueForChildName(name));
        params.put("name", nameProperty);
        params.put("nameValue", name);
        params.put("type", typeProperty);
        params.put("typeValue", type);


        Result result = tx.execute(String.format("MATCH (child)-[r:%s]->(parent)" +
                " WHERE r[$transEnd]>= $transEndVal AND ID(parent) = $idParent" +
                " AND child[$name]=$nameValue AND child[$type]=$typeValue" +
                " AND r[$edgeChild]=$edgeChildValue" +
                " RETURN child", label), params);
        return result.hasNext()
                ? result.stream()
                .map(entry -> (Node)entry.get("child"))
                .collect(Collectors.toList())
                : Collections.emptyList();
    }

    //TODO: question: it should take all adjacent nodes from
    public List<Node> getRevisionNode(Transaction tx, Node revisionRoot,
                                      RevisionInterval revisionInterval) {
        Validate.notNull(revisionRoot);
        Validate.notNull(revisionInterval);

        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("tranStart", EdgeProperty.TRAN_START.t());
        params.put("tranEnd", EdgeProperty.TRAN_END.t());
        params.put("startNodeId", revisionRoot.getId());
        params.put("revisionStart", revisionInterval.getStart());
        params.put("revisionEnd", revisionInterval.getEnd());
        params.put("relationshipTypes", List.of(MantaRelationshipType.HAS_REVISION.t()));
        Result result = tx.execute("\n" +
                "MATCH p = (s)-[]->(e) \n" +
                "WHERE id(s) = $startNodeId" +
                "    AND ALL(rel IN relationships(p) \n" +
                "        WHERE \n" +
                "           type(rel) IN $relationshipTypes\n" +
                "           AND rel[$tranStart] <= $revisionEnd AND rel[$tranEnd] >= $revisionStart  \n" +
                "    ) \n" +
                "RETURN e", params);
        return result.hasNext()
                ? result.stream()
                .map(entry -> (Node)entry.get("e"))
                .collect(Collectors.toList())
                : Collections.emptyList();
    }

    /**
     * Fetches source code
     * @param tx Database transaction
     * @param sourceRoot Source root vertex
     * @param scLocalName Source code local name
     * @param scTechnology Source code technology
     * @param scConnection Source code connection
     * @param scHash Source code hash
     * @param latestCommittedRevision Latest committed revision
     * @return Source code vertex
     */
    public Node getSourceCode(Transaction tx, Node sourceRoot, String scLocalName, String scTechnology,
                              String scConnection, String scHash, Double latestCommittedRevision) {
        Validate.notNull(sourceRoot);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idSource", sourceRoot.getId());
        params.put("transEnd", EdgeProperty.TRAN_END.t());
        params.put("transEndVal", latestCommittedRevision);
        params.put("sourceCodeTech", NodeProperty.SOURCE_NODE_TECHNOLOGY.t());
        params.put("scTechVal", scTechnology);
        params.put("sourceCodeCon", NodeProperty.SOURCE_NODE_CONNECTION.t());
        params.put("scConVal", scConnection);

        params.put("edgeLocal", EdgeProperty.SOURCE_LOCAL_NAME.t());
        params.put("edgeLocalValue", processValueForLocalName(scLocalName));
        params.put("sourceLocal", NodeProperty.SOURCE_NODE_LOCAL.t());
        params.put("sourceLocalValue", scLocalName);
        params.put("sourceHash", NodeProperty.SOURCE_NODE_HASH.t());
        params.put("sourceHashValue", scHash);

        Result result = tx.execute(String.format("MATCH (source)-[r:%s]->" +
                        "(sourceCode:%s)" +
                        " WHERE r[$transEnd] >= $transEndVal AND ID(source) = $idSource" +
                        " AND COALESCE(sourceCode[$sourceCodeTech],'')= $scTechVal" +
                        " AND COALESCE(sourceCode[$sourceCodeCon],'')= $scConVal" +
                        " AND r[$edgeLocal]=$edgeLocalValue " +
                        " AND sourceCode[$sourceLocal]=$sourceLocalValue AND sourceCode[$sourceHash]=$sourceHashValue" +
                        " RETURN sourceCode",
                MantaRelationshipType.HAS_SOURCE.t(), MantaNodeLabel.SOURCE_NODE.getVertexLabel()), params);
        return result.hasNext()? (Node)result.next().get("sourceCode"): null;
    }

    /**
     * Fetches attribute
     * @param tx Database transaction
     * @param ownerNodeId Owner node vertex id
     * @param name Name of attribute
     * @param value Value of attribute
     * @param latestCommittedRevision Latest committed revision
     * @return Attribute vertex
     */
    public Node getAttribute(Transaction tx, Long ownerNodeId, String name, Object value,
                             Double latestCommittedRevision) {
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idSource", ownerNodeId);
        params.put("transEnd", EdgeProperty.TRAN_END.t());
        params.put("transEndVal", latestCommittedRevision);

        params.put("attributeName", NodeProperty.ATTRIBUTE_NAME.t());
        params.put("attributeNameValue", name);
        params.put("attributeValue", NodeProperty.ATTRIBUTE_VALUE.t());
        params.put("attributeValueValue", value);

        Result result = tx.execute(String.format("MATCH (source)-[r:%s]->" +
                "(attribute)" +
                " WHERE r[$transEnd] >= $transEndVal AND ID(source) = $idSource" +
                " AND attribute[$attributeName]=$attributeNameValue" +
                " AND attribute[$attributeValue]=$attributeValueValue" +
                " RETURN attribute", MantaRelationshipType.HAS_ATTRIBUTE.t()), params);
        return result.hasNext()? (Node)result.next().get("attribute"): null;
    }

    /**
     * Fetches node by its id
     * @param tx Database transaction
     * @param id Node vertex id
     * @return Node
     */
    public Node getNodeById(Transaction tx, Long id) {

        Result result = tx.execute("MATCH (vertex) WHERE ID(vertex) = $idVertex RETURN vertex",
                Map.of("idVertex", id));
        return result.hasNext()? (Node)result.next().get("vertex"): null;
    }

    /**
     * Fetches possible layers of resource
     * @param tx Database transaction
     * @param resource Resource vertex
     * @param layerName Layer name
     * @param latestCommittedRevision Latest committed revision
     * @return List of nodes
     */
    public List<Node> getLayer(Transaction tx, Node resource, String layerName, Double latestCommittedRevision) {
        // Node properties
        Map<String, Object> vertexProps = new HashMap<>();
        vertexProps.put(NodeProperty.LAYER_NAME.t(), layerName);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("vertexProps", vertexProps);
        params.put("idRes", resource.getId());
        params.put("transEnd", EdgeProperty.TRAN_END.t());
        params.put("transEndVal", latestCommittedRevision);

        params.put("layerName", NodeProperty.LAYER_NAME.t());
        params.put("layerNameValue", layerName);
        Result result = tx.execute(String.format("MATCH (resource)-[r:%s]->(layer)" +
                " WHERE r[$transEnd] >= $transEndVal AND ID(resource) = $idRes" +
                " AND layer[$layerName]=$layerNameValue" +
                " RETURN layer", MantaRelationshipType.IN_LAYER.t()), params);
        return result.hasNext()
                ? result.stream()
                .map(entry -> (Node)entry.get("layer"))
                .collect(Collectors.toList())
                : Collections.emptyList();
    }

    /**
     * Set transaction end of a subtree. All vertices in the subtree (and their edges) valid in a specific (walk-through)
     * revision will have their endRevision set to a new value. Subtree is specified by its root vertex. Vertices and edges
     * absent in the specified (walk-through) revision are ignored.
     * <p>
     * WARNIGN: If vertex' or edge's new end revision (new tranEnd) is less than its start revision (tranStart),
     * the vertex or the edge is REMOVED from the repository (together with its whole subtree). In case of hasAttribute
     * edge, not only the edge but also the nodeAttribute is removed.
     * <p>
     * Example:
     * _________________________________________________________________________________________________________
     * <1.000000, 1.999999>                 <1.000000, 1.999999>
     * nodeA-----------hasAttribute-------->attrA
     * |
     * +-------------------------------+
     * |                               |
     * <1.000000, 1.111111>            <1.000000, 1.999999>    <1.000000, 1.999999>      <1.000000, 1.999999>
     * nodeB                           nodeC----------------directFlow------------------>nodeD
     * <p>
     * _________________________________________________________________________________________________________
     * Before
     * <p>
     * -----------------------------------------------------
     * setSubtreeTransactionEnd(nodeA, 1.555555, 1.555555)
     * -----------------------------------------------------
     * _________________________________________________________________________________________________________
     * <1.000000, 1.555555>                 <1.000000, 1.555555>
     * nodeA-----------hasAttribute-------->attrA
     * |
     * +-------------------------------+
     * |                               |
     * <1.000000, 1.111111>            <1.000000, 1.555555>    <1.000000, 1.555555>      <1.000000, 1.999999>
     * nodeB                           nodeC----------------directFlow------------------>nodeD
     * _________________________________________________________________________________________________________
     * After
     * <p>
     * Notice from the example above:
     * 1) nodeB absent in the revision 1.555555 is ignored (its tranEnd remains unchanged)
     * 2) directFlow between nodeC and nodeD has its tranEnd changed (since it is connected to the affected nodeC)
     * 3) nodeD is ignored, since it is NOT in the subtree with the root nodeA
     *
     * Query traverses from source vertex to all leafs (attribute nodes) and updates end revisions of all
     * relationships of all vertices on paths. Paths are then traversed here to validate start revisions
     * of all control edges - if start revision is current merged revision the node with its subtree and all relationships
     * linked to that subtree has to be deleted
     *
     * @param tx Database transaction
     * @param rootVertex Vertex whose subtree is changed
     * @param parentVertexId Parent vertex of rootVertex
     * @param newEndRevision New end revision number
     */
    public void setSubtreeTransactionEnd(Transaction tx, Node rootVertex, Long parentVertexId,
                                         Double newEndRevision) {
        Validate.notNull(rootVertex, "The argument 'rootVertex' was null.");
        Validate.notNull(newEndRevision, "The argument 'newEndRevision' was null.");
        RevisionInterval walkthroughRevInterval = new RevisionInterval(newEndRevision, newEndRevision);

        if (!revisionUtils.isVertexInRevisionInterval(tx, rootVertex, walkthroughRevInterval)) {
            throw new IllegalStateException(
                    "The argument rootNode does not exist in the given revision " + newEndRevision);
        }
        Result result = tx.execute(String.format("MATCH (child)-[r:%s|%s]->(parent)" +
                        " WHERE ID(child)=$idChild AND ID(parent)=$idParent" +
                        " RETURN r", MantaRelationshipType.HAS_PARENT.t(), MantaRelationshipType.HAS_RESOURCE.t()),
                Map.of("idChild", rootVertex.getId(), "idParent", parentVertexId));
        Relationship controlRelationship = result.hasNext()? (Relationship) result.next().get("r"): null;
        if (!revisionUtils.isEdgeInRevisionInterval(controlRelationship, walkthroughRevInterval)) {
            throw new IllegalStateException(
                    "The argument rootNode does not exist in the given revision " + newEndRevision);
        }
        if (newEndRevision < Double.parseDouble(controlRelationship.getProperty(EdgeProperty.TRAN_START.t()).toString())) {
            deleteSubtree(tx, rootVertex.getId());
            return;
        }

        // Edge properties
        Map<String, Object> edgeProps = new HashMap<>();
        edgeProps.put(EdgeProperty.TRAN_END.t(), newEndRevision);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idNode", rootVertex.getId());
        params.put("edgeProps", edgeProps);
        params.put("vertexType", NodeProperty.VERTEX_TYPE.t());
        params.put("vertexAttributeVal", MantaNodeLabel.ATTRIBUTE.toString());
        params.put("vertexNodeVal", MantaNodeLabel.NODE.toString());

        Result pathResult = tx.execute(String.format("MATCH path = (m)<-[:%s*0..]-(b)-[:%s*0..1]->(a)" +
                        " WHERE id(m)=$idNode AND (nodes(path)[size(nodes(path))-1][$vertexType]=$vertexAttributeVal" +
                        " OR (nodes(path)[size(nodes(path))-1][$vertexType]= $vertexNodeVal" +
                        " AND NOT (a)<-[:%s]-() AND NOT (a)-[:%s]->()))" +
                        " UNWIND nodes(path) as allNodes" +
                        " MATCH (allNodes)-[rels]-()" +
                        " SET rels += $edgeProps" +
                        " return path",
                MantaRelationshipType.HAS_PARENT.t(), MantaRelationshipType.HAS_ATTRIBUTE.t(),
                MantaRelationshipType.HAS_PARENT.t(), MantaRelationshipType.HAS_ATTRIBUTE.t()), params);

        Set<Long> deletedNodes = new HashSet<>();
        while (pathResult.hasNext())  {
            Path path = (Path)pathResult.next().get("path");
            for (Relationship relationship : path.relationships()) {
                if (deletedNodes.contains(relationship.getStartNodeId())
                        || deletedNodes.contains(relationship.getEndNodeId())) {
                    break;
                }
                if (!revisionUtils.isEdgeInRevisionInterval(relationship, walkthroughRevInterval)) {
                    throw new IllegalStateException(
                            "The argument rootNode does not exist in the given revision " + newEndRevision);
                }
                if (newEndRevision < Double.parseDouble(relationship.getProperty(EdgeProperty.TRAN_START.t()).toString())) {
                    long nodeIdToDelete = -1;
                    if (relationship.getType().equals(MantaRelationshipType.HAS_PARENT.t())) {
                        nodeIdToDelete = relationship.getStartNodeId();
                    } else if (relationship.getType().equals(MantaRelationshipType.HAS_ATTRIBUTE.t())) {
                        nodeIdToDelete = relationship.getEndNodeId();
                    }
                    deleteSubtree(tx, nodeIdToDelete);
                    deletedNodes.add(nodeIdToDelete);
                    break;
                }
            }
        }
    }

    /**
     * Delete all vertices and edges (regardless their revision validity) in a subtree defined by a root node.
     * Root node is removed as well.
     *
     * @param rootNode  root node of the subtree
     */
    public void deleteSubtree(Transaction tx, Long rootNode) {
        //TODO - needs to be tested
        Validate.notNull(rootNode, "The argument 'rootNode' was null.");
        Map<String,Object> params = new HashMap<>();
        params.put("idNode", rootNode);
        params.put("vertexType", NodeProperty.VERTEX_TYPE.t());
        params.put("vertexAttributeVal", MantaNodeLabel.ATTRIBUTE.toString());
        params.put("vertexNodeVal", MantaNodeLabel.NODE.toString());
        tx.execute( String.format("MATCH path = (a)<-[:%s*0..1]-(b)-[:%s*0..]->(m {type:'node'})" +
                        " WHERE id(m)=$idNode AND (nodes(path)[0][$vertexType]=$vertexAttributeVal" +
                        " OR (nodes(path)[0][$vertexType]=$vertexNodeVal" +
                        " AND NOT (a)<-[:%s]-() AND NOT (a)-[:%s]->()))" +
                        " UNWIND nodes(path) as allNodes" +
                        " DETACH DELETE allNodes",
                MantaRelationshipType.HAS_ATTRIBUTE.t(), MantaRelationshipType.HAS_PARENT.t(),
                MantaRelationshipType.HAS_PARENT.t(), MantaRelationshipType.HAS_ATTRIBUTE.t()),
                params);
    }

    /**
     * Fetches parent of node
     * @param tx Database transaction
     * @param node Node vertex
     * @return Parent vertex or null if node has no parent
     */
    public Node getParent(Transaction tx, Node node) {
        Validate.notNull(node, "The argument 'rootNode' was null.");
        Result result = tx.execute(String.format("MATCH (node)-[:%s]->(parent)" +
                        " WHERE ID(node)=$idNode" +
                        " RETURN parent", MantaRelationshipType.HAS_PARENT.t()),
                Map.of("idNode", node.getId()));
        return result.hasNext()? (Node)result.next().get("parent"): null;
    }

    /**
     * Gets vertex by qualified name
     * @param tx Database transaction
     * @param root Root vertex
     * @param qn List of qualified triplets (resource name, name, type)
     * @param revisionInterval Revision interval
     * @return Node or null if node not found
     */
    public Node getVertexByQualifiedName(Transaction tx, Node root, List<List<String>> qn,
                                         RevisionInterval revisionInterval) {
        List<Node> vertexPath = getVertexPathFromQualifiedNameImperative(tx, root, qn, revisionInterval);
        if (vertexPath == null || vertexPath.isEmpty()) {
            return null;
        }
        return vertexPath.get(vertexPath.size() - 1);
    }

    public List<Node> getVertexPathFromQualifiedNameImperative(Transaction tx, Node root, List<List<String>> qn,
                                                     RevisionInterval revisionInterval) {
        if (qn == null || qn.isEmpty()) {
            return List.of();
        }

        List<Node> finalPath = new ArrayList<>();
        Node nextNode = null;
        if (qn.get(0).size() != QUALIFIED_NAME_PARTS) {
            return List.of();
        }
        Result result = getResource(tx, root.getId(), qn.get(0).get(2), revisionInterval);
        if (!result.hasNext()) return List.of();
        nextNode = (Node)result.next().get("n");
        String resourceName = (String)nextNode.getProperty(NodeProperty.RESOURCE_NAME.t());
        finalPath.add(nextNode);
        for (int i = 1; i < qn.size(); ++i) {
            if (qn.get(i).size() != QUALIFIED_NAME_PARTS) {
                return List.of();
            }
            result = getChildren(tx, nextNode.getId(),
                    NodeProperty.NODE_NAME.t(), NodeProperty.NODE_TYPE.t(),
                    qn.get(i).get(0), qn.get(i).get(1), revisionInterval);
            if (!result.hasNext()) return List.of();
            List<Node> foundExactVertices = new ArrayList<>();
            List<Node> foundInsensitiveVertices = new ArrayList<>();
            while (result.hasNext()) {
                Map<String,Object> resultMap = result.next();
                nextNode = (Node)resultMap.get("n");
                Object resource = resultMap.get("r");
                String childResourceName = resource != null
                        ? (String)((Node)resource).getProperty(NodeProperty.RESOURCE_NAME.t())
                        : resourceName;
                String name = (String)nextNode.getProperty(NodeProperty.NODE_NAME.t());
                String type = (String)nextNode.getProperty(NodeProperty.NODE_TYPE.t());

                if (StringUtils.equals(qn.get(i).get(0), name)
                        && (StringUtils.isEmpty(type) || StringUtils.equals(qn.get(i).get(1), type))
                        && (StringUtils.isEmpty(resourceName) || StringUtils.equals(childResourceName, resourceName))) {
                    foundExactVertices.add(nextNode);
                }
                else if (StringUtils.equalsIgnoreCase(qn.get(i).get(0), name)
                        && (StringUtils.isEmpty(type) || StringUtils.equalsIgnoreCase(qn.get(i).get(1), type))
                        && (StringUtils.isEmpty(resourceName) || StringUtils.equalsIgnoreCase(childResourceName, resourceName))) {
                    foundInsensitiveVertices.add(nextNode);
                }
            }
            if (foundExactVertices.size() == 1) {
                nextNode = foundExactVertices.get(0);
            } else if (foundExactVertices.size() > 1) {
                LOGGER.warn("There exist {} vertices for identification {} {} {}.", foundExactVertices.size(),
                        qn.get(i).get(0), qn.get(i).get(1), resourceName);
                return List.of();
            } else if (foundInsensitiveVertices.size() == 1) {
                nextNode = foundInsensitiveVertices.get(0);
            } else if (foundInsensitiveVertices.size() > 1) {
                LOGGER.warn("There exist {} vertices for identification {} {} {}.", foundInsensitiveVertices.size(),
                        qn.get(i).get(0), qn.get(i).get(1), resourceName);
                return null;
            } else {
                return List.of();
            }
            finalPath.add(nextNode);
        }
        return finalPath;
    }

    private Result getChildren(Transaction tx, Long nodeId,
                               String nameProperty, String typeProperty,
                               String namePropertyValue, String typePropertyValue,
                               RevisionInterval revisionInterval) {
        Map<String, Object> params = new HashMap<>();
        params.put("id", nodeId);
        params.put("transStartProp", EdgeProperty.TRAN_START.t());
        params.put("transEndProp", EdgeProperty.TRAN_END.t());
        params.put("tranStart", revisionInterval.getEnd());
        params.put("tranEnd", revisionInterval.getStart());
        params.put("nodeName", nameProperty);
        params.put("nodeType", typeProperty);
        params.put("nodeNameVal", namePropertyValue);
        params.put("nodeTypeVal", typePropertyValue);
        return tx.execute(String.format(
                    "MATCH (p)<-[rel:%s|%s]-(n:Node)\n" +
                    "   WHERE ID(p)=$id\n" +
                    "       AND rel[$transStartProp] <= $tranStart\n" +
                    "       AND rel[$transEndProp] >= $tranEnd\n" +
                    "       AND TOLOWER(n[$nodeName])=TOLOWER($nodeNameVal)\n" +
                    "       AND TOLOWER(n[$nodeType])=TOLOWER($nodeTypeVal)\n" +
                    " OPTIONAL MATCH (n)-[:%s]->(r)\n" +
                    "RETURN n,r",
                MantaRelationshipType.HAS_PARENT.t(),
                MantaRelationshipType.HAS_RESOURCE.t(),
                MantaRelationshipType.HAS_RESOURCE.t()), params);
    }

    private Result getResource(Transaction tx, Long nodeId,
                               String namePropertyValue, RevisionInterval revisionInterval) {
        Map<String, Object> params = new HashMap<>();
        params.put("id", nodeId);
        params.put("transStartProp", EdgeProperty.TRAN_START.t());
        params.put("transEndProp", EdgeProperty.TRAN_END.t());
        params.put("tranStart", revisionInterval.getEnd());
        params.put("tranEnd", revisionInterval.getStart());
        params.put("nodeName", NodeProperty.RESOURCE_NAME.t());
        params.put("nodeNameVal", namePropertyValue);
        return tx.execute(String.format(
                "MATCH (p)<-[r:%s]-(n:Resource)\n" +
                "   WHERE ID(p)=$id\n" +
                "       AND r[$transStartProp] <= $tranStart\n" +
                "       AND r[$transEndProp] >= $tranEnd\n" +
                "       AND TOLOWER(n[$nodeName])=TOLOWER($nodeNameVal)\n" +
                "RETURN n", MantaRelationshipType.HAS_RESOURCE.t()), params);
    }

    /**
     * Fetches vertex by its qualified name (list of three valued elements - name, type, resource name).
     * First string array of qualified name and types is created which is input to query as parameter.
     * Then paths fulfilling name and type criteria (case insensitive equality) from root vertex are fetched and then only 1 correct path is selected.
     * If at one point there are 2 or more paths containing same vertex (name and type case sensitive equality) null is returned.
     * If at one point there is no case sensitive match, but there are 2 or more paths containing.
     * same vertex (name and type case insensitive equality) null is returned.
     *
     * @param tx Database transaction
     * @param root Root vertex
     * @param qn List of qualified triplets (resource name, name, type)
     * @param revisionInterval Revision interval
     * @return List of nodes on path
     */
    public List<Node> getVertexPathFromQualifiedName(Transaction tx, Node root, List<List<String>> qn,
                                                     RevisionInterval revisionInterval) {
        if (qn == null || qn.isEmpty()) {
            return null;
        }
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idSuperRoot", root.getId());

        params.put("resNameProp", NodeProperty.RESOURCE_NAME.t());
        params.put("resName", qn.get(0).get(2));

        params.put("transStartProp", EdgeProperty.TRAN_START.t());
        params.put("transEndProp", EdgeProperty.TRAN_END.t());
        params.put("tranStart", revisionInterval.getEnd());
        params.put("tranEnd", revisionInterval.getStart());

        params.put("nodeName", NodeProperty.NODE_NAME.t());
        params.put("nodeType", NodeProperty.NODE_TYPE.t());

        params.put("qn", qn);
        Result result = tx.execute(String.format("WITH $qn AS path" +
                " MATCH p = (superRoot)<-[rel:%s]-(res)<-[*%d]-()" +
                " WHERE ID(superRoot) = $idSuperRoot AND toLower(res[$resNameProp])=toLower($resName)" +
                    " AND rel[$transStartProp] <= $tranStart" +
                    " AND rel[$transEndProp] >= $tranEnd" +
                    " AND ALL(idx in range(2, size(nodes(p))-1)" +
                        " WHERE toLower(coalesce(nodes(p)[idx][$nodeName], '')) = toLower(path[idx-1][0])" +
                            " AND toLower(coalesce(nodes(p)[idx][$nodeType], '')) = toLower(path[idx-1][1])" +
                            " AND relationships(p)[idx-1][$transStartProp] <= $tranStart" +
                            " AND relationships(p)[idx-1][$transEndProp] >= $tranEnd) " +
                " UNWIND nodes(p) AS n" +
                " MATCH resPaths = (n)-[:hasResource]->(r)" +
                " RETURN p, COLLECT(resPaths) as nodeToResourcePaths",
                MantaRelationshipType.HAS_RESOURCE.t(), qn.size() - 1), params);

        List<Node> finalPath = new ArrayList<>();
        Map<Long, String> nodeToResourceName = new HashMap<>();
        Map<Integer, String> pathToResource = new HashMap<>();
        int k = 0;
        Map<Integer, Iterator<Node>> nodePaths = new HashMap<>();
        if (result.hasNext()) {
            Map<String, Object> resultMap = result.next();
            Path path = (Path)resultMap.get("p");
            List<Path> nodeToResourcePaths = (List<Path>)resultMap.get("nodeToResourcePaths");
            nodeToResourcePaths.forEach(nodeToResourcePath-> {
                Node endNode = nodeToResourcePath.endNode();
                if (endNode.hasProperty(NodeProperty.RESOURCE_NAME.t())) {
                    nodeToResourceName
                            .put(nodeToResourcePath.startNode().getId(),
                                    (String) nodeToResourcePath.endNode().getProperty(NodeProperty.RESOURCE_NAME.t()));
                }
            });
            Iterator<Node> it = path.nodes().iterator();
            it.next();
            Node resource = it.next();
            pathToResource.put(k, (String)resource.getProperty(NodeProperty.RESOURCE_NAME.t()));
            nodePaths.put(k++, it);
        }
        // Skip resource (start with 2. node) - in each step select correct path
        for (int i = 1; i < qn.size(); ++i) {
            //Found vertices
            Set<Node> foundExactVertices = new HashSet<>();
            Set<Node> foundInsensitiveVertices = new HashSet<>();
            //Paths to preserve in next iteration
            Set<Integer> sensitiveMatchPaths = new HashSet<>();
            Set<Integer> insensitiveMatchPaths = new HashSet<>();
            String name = qn.get(i).get(0);
            String type = qn.get(i).get(1);
            String resourceName = qn.get(i).get(2);
            boolean foundSensitive = false;
            for (Map.Entry<Integer, Iterator<Node>> entry : nodePaths.entrySet()) {
                Node examined = entry.getValue().next();
                String examinedResourceName = nodeToResourceName.getOrDefault(examined.getId(),
                        pathToResource.get(entry.getKey()));

                if (StringUtils.equals(examined.getProperties(NodeProperty.NODE_NAME.t()).toString(), name)
                        && (StringUtils.isEmpty(type) ||
                        StringUtils.equals(examined.getProperty(NodeProperty.NODE_TYPE.t()).toString(), type))
                        && (StringUtils.isEmpty(resourceName) || StringUtils.equals(examinedResourceName, resourceName))) {
                    if (i > 1 || getParent(tx, examined) == null) {
                        foundExactVertices.add(examined);
                        sensitiveMatchPaths.add(entry.getKey());
                    }
                } else if (i > 1 || getParent(tx, examined) == null) {
                    foundInsensitiveVertices.add(examined);
                    insensitiveMatchPaths.add(entry.getKey());
                }
                if (foundExactVertices.size() == 1) {
                    finalPath.add(foundExactVertices.iterator().next());
                    foundSensitive = true;
                } else if (foundExactVertices.size() > 1) {
                    LOGGER.warn("There exist {} vertices for identification {} {} {}.", foundExactVertices.size(), name, type,
                            resourceName);
                    return null;
                } else if (foundInsensitiveVertices.size() == 1) {
                    finalPath.add(foundInsensitiveVertices.iterator().next());
                    foundSensitive = false;
                } else if (foundInsensitiveVertices.size() > 1) {
                    LOGGER.warn("There exist {} vertices for identification {} {} {}.", foundInsensitiveVertices.size(), name, type,
                            resourceName);
                    return null;
                } else {
                    return null;
                }
            }
            //remove all non-matching paths
            if (foundSensitive) {
                nodePaths.keySet().removeIf(key -> !sensitiveMatchPaths.contains(key));
            } else {
                nodePaths.keySet().removeIf(key -> !insensitiveMatchPaths.contains(key));
            }
            sensitiveMatchPaths.clear();
            insensitiveMatchPaths.clear();
        }
        return finalPath;
    }

    /**
     * Gets vertex path as a string
     * @param tx Database transaction
     * @param nodeId Node vertex id
     * @return Vertex path as string
     */
    public String getVertexPathString(Transaction tx, Long nodeId) {
        if (nodeId == null) {
            return null;
        }
        List<Node> path = getVertexPath(tx, nodeId);
        return getVertexPathString(path);
    }

    /**
     * Fetches vertex path from its resource
     * @param tx Database transaction
     * @param nodeId Node vertex id
     * @return List of nodes representing vertex path
     */
    private List<Node> getVertexPath(Transaction tx, Long nodeId) {
        if (nodeId == null) throw new IllegalArgumentException("The argument vertex was null.");
        List<Node> nodePath = new ArrayList<>();
        Result result = tx.execute(String.format(
                        "MATCH path=(resource:Resource)<-[:%s]-()<-[:%s*0..]-(node)\n" +
                        "WHERE ID(node)=$idNode\n" +
                        "RETURN path ORDER BY LENGTH(path) ASC LIMIT 1",
                MantaRelationshipType.HAS_RESOURCE.t(),
                MantaRelationshipType.HAS_PARENT.t()),
                Map.of("idNode", nodeId));
        if (result.hasNext()) {
            List<Map<String, Object>> records = result.stream()
                    .collect(Collectors.toList());
            Map<String, Object> record = records.get(records.size() - 1);
            Path path = (Path)record.get("path");
            for (Node nodeInPath : path.nodes()) {
                if (StoredMantaNodeLabelConverter.getType(nodeInPath) != MantaNodeLabel.ATTRIBUTE) {
                    nodePath.add(nodeInPath);
                }
            }
        } else {
            LOGGER.warn("Could not fetch vertex path for vertex with id:{}.", nodeId);
            throw new IllegalStateException("Could not fetch vertex path for vertex with id " + nodeId);
        }
        return nodePath;
    }

    /**
     * Returns names of all nodes from the specified list including the top level resource of the node and the node itself.
     * The top level resource is first element of the list.
     *
     * @return Path string from top-level resource or {@code null} if the path is {@code null}.
     */
    public String getVertexPathString(List<Node> path) {
        if (path == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        path.forEach(vertex -> {
            if (sb.length() > 0) {
                sb.append(".");
            }
            sb.append(String.format("%s[%s]", getName(vertex), getType(vertex)));
        });
        return sb.toString();
    }

    /**
     * Validates whether there can exist relationship of given label between given vertices.
     *
     * @param sourceVertex Source vertex
     * @param targetVertex Target vertex
     * @param label        Laabel of the edge
     * @return Flag whether edge label is valid.
     */
    public boolean isMantaRelationshipTypeCorrect(Node sourceVertex, Node targetVertex, MantaRelationshipType label) {
        MantaNodeLabel sourceType = StoredMantaNodeLabelConverter.getType(sourceVertex);
        MantaNodeLabel targetType = StoredMantaNodeLabelConverter.getType(targetVertex);

        switch (label) {
            case DIRECT:
            case FILTER:
            case HAS_PARENT:
            case MAPS_TO:
            case PERSPECTIVE:
                if (sourceType == MantaNodeLabel.NODE && targetType == MantaNodeLabel.NODE) {
                    return true;
                }
                break;
            case HAS_ATTRIBUTE:
                if (sourceType == MantaNodeLabel.NODE && targetType == MantaNodeLabel.ATTRIBUTE) {
                    return true;
                }
                break;
            case HAS_RESOURCE:
                if ((sourceType == MantaNodeLabel.NODE && targetType == MantaNodeLabel.RESOURCE)
                        || (sourceType == MantaNodeLabel.RESOURCE && targetType == MantaNodeLabel.SUPER_ROOT)) {
                    return true;
                }
                break;
            case HAS_REVISION:
                if (sourceType == MantaNodeLabel.REVISION_ROOT && targetType == MantaNodeLabel.REVISION_NODE) {
                    return true;
                }
                break;
            case HAS_SOURCE:
                if (sourceType == MantaNodeLabel.SOURCE_ROOT && targetType == MantaNodeLabel.SOURCE_NODE) {
                    return true;
                }
                break;
            case IN_LAYER:
                if (sourceType == MantaNodeLabel.RESOURCE && targetType == MantaNodeLabel.LAYER) {
                    return true;
                }
                break;
            default:
                throw new IllegalStateException("Unknown label " + label + ".");
        }
        return false;
    }

    /**
     * Processes the value in case of {@link EdgeProperty#CHILD_NAME}.
     *
     * @param value String to process
     * @return Processed string.
     */
    public String processValueForChildName(String value) {
        if (value != null) {
            return value.length() > 200 ? value.substring(0, 200) : value;
        } else {
            return null;
        }
    }

    /**
     * Processes the value in case of {@link EdgeProperty#SOURCE_LOCAL_NAME}.
     *
     * @param localName String to process
     * @return Processed string.
     */
    public String processValueForLocalName(String localName) {
        return String.valueOf(localName.toLowerCase().hashCode());
    }
}
