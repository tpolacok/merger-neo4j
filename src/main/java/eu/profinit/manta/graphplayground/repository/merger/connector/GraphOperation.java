package eu.profinit.manta.graphplayground.repository.merger.connector;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import eu.profinit.manta.graphplayground.model.manta.core.EdgeIdentification;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.model.manta.util.DriverMantaNodeLabelConverter;
import eu.profinit.manta.graphplayground.repository.merger.permission.RepositoryPermissionProvider;
import eu.profinit.manta.graphplayground.repository.merger.revision.RevisionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

import static org.neo4j.driver.Values.parameters;

/**
 * Class containing methods for working with graph objects.
 * Contains methods for traversing and retrieval operations.
 *
 * @author tpolacok
 */
@Repository
public class GraphOperation {

    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphOperation.class);

    /**
     * Decimal format with 6 decimal digits for printing
     */
    private static NumberFormat formatter = new DecimalFormat("#0.000000");

    /**
     * Number of parts of qualified name
     */
    private static final int QUALIFIED_NAME_PARTS = 3;

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

        MantaNodeLabel type = DriverMantaNodeLabelConverter.getType(vertex);
        switch (type) {
            case LAYER:
                return vertex.get(DatabaseStructure.NodeProperty.LAYER_TYPE.t()).asString();
            case NODE:
                return vertex.get(DatabaseStructure.NodeProperty.NODE_TYPE.t()).asString();
            case RESOURCE:
                return vertex.get(DatabaseStructure.NodeProperty.RESOURCE_TYPE.t()).asString();
            case ATTRIBUTE:
                return vertex.get(DatabaseStructure.NodeProperty.ATTRIBUTE_NAME.t()).asString();
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
        MantaNodeLabel type = DriverMantaNodeLabelConverter.getType(vertex);
        switch (type) {
            case LAYER:
                return vertex.get(DatabaseStructure.NodeProperty.LAYER_NAME.t()).asString();
            case NODE:
                return vertex.get(DatabaseStructure.NodeProperty.NODE_NAME.t()).asString();
            case RESOURCE:
                return vertex.get(DatabaseStructure.NodeProperty.RESOURCE_NAME.t()).asString();
            case ATTRIBUTE:
                return vertex.get(DatabaseStructure.NodeProperty.ATTRIBUTE_NAME.t()).asString();
            case SUPER_ROOT:
                return MantaNodeLabel.SUPER_ROOT.toString();
            case REVISION_ROOT:
                return MantaNodeLabel.REVISION_ROOT.toString();
            case REVISION_NODE:
                return "revision[" + formatter.format(vertex.get(DatabaseStructure.NodeProperty.REVISION_NODE_REVISION.t()).toString()) + "]";
            case SOURCE_ROOT:
                return MantaNodeLabel.SOURCE_ROOT.toString();
            case SOURCE_NODE:
                return vertex.get(DatabaseStructure.NodeProperty.SOURCE_NODE_LOCAL.t()).toString();
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
        Result result = tx.run(String.format("MATCH (node)<-[:%s*0..1]-" +
                        "()-[:%s*0..]->()-[:%s]->(resource)" +
                        " WHERE ID(node)=$idNode " +
                        " RETURN CASE WHEN $resourceLabel in labels(node)" +
                        " THEN node ELSE resource END as resource LIMIT 1",
                DatabaseStructure.MantaRelationshipType.HAS_ATTRIBUTE.t(), DatabaseStructure.MantaRelationshipType.HAS_PARENT.t(), DatabaseStructure.MantaRelationshipType.HAS_RESOURCE.t()),
                params);
        if (result.hasNext()) {
            return result.single().get("resource").asNode();
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
        Result result = tx.run(String.format("MATCH (node) WHERE ID(node)=$idNode" +
                        " WITH node" +
                        " OPTIONAL MATCH (node)<-[:%s*0..1]-()-[:%s*0..]->()-[:%s]->(resource)-[:%s]->(layer)" +
                        " WITH node, layer" +
                        " OPTIONAL MATCH (node)-[:inLayer]->(layer2)" +
                        " RETURN CASE WHEN $layerLabel in labels(node) THEN node" +
                        " WHEN $resourceLabel in labels(node) THEN layer2" +
                        " ELSE layer" +
                        " END as layer LIMIT 1",
                DatabaseStructure.MantaRelationshipType.HAS_ATTRIBUTE.t(),
                DatabaseStructure.MantaRelationshipType.HAS_PARENT.t(),
                DatabaseStructure.MantaRelationshipType.HAS_RESOURCE.t(),
                DatabaseStructure.MantaRelationshipType.IN_LAYER.t()),
                params);
        if (result.hasNext()) {
            return result.single().get("layer").asNode();
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
        Result rel = tx.run("MATCH ()-[r]->() WHERE ID(r)=$idRelation RETURN r",
                parameters("idRelation", edgeIdentification.getRelationId()));
        return rel.hasNext() ? rel.single().get("r").asRelationship(): null;
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
        params.put("transStart", DatabaseStructure.EdgeProperty.TRAN_START.t());
        params.put("transStartVal", revisionInterval.getEnd());
        params.put("transEnd", DatabaseStructure.EdgeProperty.TRAN_END.t());
        params.put("transEndVal", revisionInterval.getStart());

        Result rel = tx.run(String.format("MATCH (source)-[r:%s]-(target)" +
                        " WHERE r[$transStart] <= $transStartVal AND r[$transEnd] >= $transEndVal" +
                        " AND ID(source) = $idSource AND ID(target) = $idTarget" +
                        " RETURN r", edgeIdentification.getLabel().t()), params);
        return rel.hasNext()? rel.single().get("r").asRelationship(): null;
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
        params.put("transEnd", DatabaseStructure.EdgeProperty.TRAN_END.t());
        params.put("transEndVal", latestCommittedRevision);

        params.put("edgeTarget", DatabaseStructure.EdgeProperty.TARGET_ID.t());
        params.put("edgeTargetValue", targetId);

        Result result = tx.run("MATCH (source)-[r:" + label + "]->(target)" +
                        " WHERE r[$transEnd] >= $transEndVal AND ID(target) = $idTarget AND ID(source) = $idSource" +
                        " AND r[$edgeTarget]=$edgeTargetValue" +
                        " RETURN r", params);
        if (result.hasNext()) {
            return result.list(r -> r.get("r").asRelationship());
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
        params.put("transEnd", DatabaseStructure.EdgeProperty.TRAN_END.t());
        params.put("transEndVal", latestCommittedRevision);
        Result result = tx.run(String.format("MATCH (source)-[r:%s]->(target)" +
                        " WHERE r[$transEnd] >= $transEndVal AND ID(source) = $idSource" +
                        " RETURN r", label), params);
        if (result.hasNext()) {
            return result.list(r -> r.get("r").asRelationship());
        } else return Collections.emptyList();
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
        params.put("transEnd", DatabaseStructure.EdgeProperty.TRAN_END.t());
        params.put("transEndVal", latestCommittedRevision);

        params.put("edgeChild", DatabaseStructure.EdgeProperty.CHILD_NAME.t());
        params.put("edgeChildValue", processValueForChildName(name));
        params.put("name", nameProperty);
        params.put("nameValue", name);
        params.put("type", typeProperty);
        params.put("typeValue", type);


        Result result = tx.run(String.format("MATCH (child)-[r:%s]->(parent)" +
                        " WHERE r[$transEnd]>= $transEndVal AND ID(parent) = $idParent" +
                        " AND child[$name]=$nameValue AND child[$type]=$typeValue" +
                        " AND r[$edgeChild]=$edgeChildValue" +
                        " RETURN child", label), params);
        return result.hasNext()? result.list(r -> r.get("child").asNode()): Collections.emptyList();
    }

    //TODO: question: it should take all adjacent nodes from
    public List<Node> getRevisionNode(Transaction tx, Node revisionRoot, RepositoryPermissionProvider repositoryPermissionProvider,
                                             RevisionInterval revisionInterval) {
        Validate.notNull(revisionRoot);
        Validate.notNull(revisionInterval);
        Validate.notNull(repositoryPermissionProvider);

        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("tranStart", DatabaseStructure.EdgeProperty.TRAN_START.t());
        params.put("tranEnd", DatabaseStructure.EdgeProperty.TRAN_END.t());
        params.put("startNodeId", revisionRoot.id());
        params.put("revisionStart", revisionInterval.getStart());
        params.put("revisionEnd", revisionInterval.getEnd());
        params.put("relationshipTypes", List.of(DatabaseStructure.MantaRelationshipType.HAS_REVISION.t()));
        Result result = tx.run("\n" +
                        "MATCH p = (s)-[]->(e) \n" +
                        "WHERE id(s) = $startNodeId" +
                        "    AND ALL(rel IN relationships(p) \n" +
                        "        WHERE \n" +
                        "           type(rel) IN $relationshipTypes\n" +
                        "           AND rel[$tranStart] <= $revisionEnd AND rel[$tranEnd] >= $revisionStart  \n" +
                        "    ) \n" +
                        "RETURN e", params);
        return result.hasNext() ? result.list(r -> r.get(0).asNode()) : Collections.emptyList();
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
        params.put("idSource", sourceRoot.id());
        params.put("transEnd", DatabaseStructure.EdgeProperty.TRAN_END.t());
        params.put("transEndVal", latestCommittedRevision);
        params.put("sourceCodeTech", DatabaseStructure.NodeProperty.SOURCE_NODE_TECHNOLOGY.t());
        params.put("scTechVal", scTechnology);
        params.put("sourceCodeCon", DatabaseStructure.NodeProperty.SOURCE_NODE_CONNECTION.t());
        params.put("scConVal", scConnection);

        params.put("edgeLocal", DatabaseStructure.EdgeProperty.SOURCE_LOCAL_NAME.t());
        params.put("edgeLocalValue", processValueForLocalName(scLocalName));
        params.put("sourceLocal", DatabaseStructure.NodeProperty.SOURCE_NODE_LOCAL.t());
        params.put("sourceLocalValue", scLocalName);
        params.put("sourceHash", DatabaseStructure.NodeProperty.SOURCE_NODE_HASH.t());
        params.put("sourceHashValue", scHash);

        Result result = tx.run(String.format("MATCH (source)-[r:%s]->" +
                        "(sourceCode:%s)" +
                        " WHERE r[$transEnd] >= $transEndVal AND ID(source) = $idSource" +
                        " AND COALESCE(sourceCode[$sourceCodeTech],'')= $scTechVal" +
                        " AND COALESCE(sourceCode[$sourceCodeCon],'')= $scConVal" +
                        " AND r[$edgeLocal]=$edgeLocalValue " +
                        " AND sourceCode[$sourceLocal]=$sourceLocalValue AND sourceCode[$sourceHash]=$sourceHashValue" +
                        " RETURN sourceCode",
                DatabaseStructure.MantaRelationshipType.HAS_SOURCE.t(), MantaNodeLabel.SOURCE_NODE.getVertexLabel()), params);
        return result.hasNext()? result.single().get("sourceCode").asNode(): null;
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
        params.put("transEnd", DatabaseStructure.EdgeProperty.TRAN_END.t());
        params.put("transEndVal", latestCommittedRevision);

        params.put("attributeName", DatabaseStructure.NodeProperty.ATTRIBUTE_NAME.t());
        params.put("attributeNameValue", name);
        params.put("attributeValue", DatabaseStructure.NodeProperty.ATTRIBUTE_VALUE.t());
        params.put("attributeValueValue", value);

        Result result = tx.run(String.format("MATCH (source)-[r:%s]->" +
                        "(attribute)" +
                        " WHERE r[$transEnd] >= $transEndVal AND ID(source) = $idSource" +
                        " AND attribute[$attributeName]=$attributeNameValue" +
                        " AND attribute[$attributeValue]=$attributeValueValue" +
                        " RETURN attribute", DatabaseStructure.MantaRelationshipType.HAS_ATTRIBUTE.t()), params);
        return result.hasNext()? result.single().get("attribute").asNode(): null;
    }

    /**
     * Fetches node by its id
     * @param tx Database transaction
     * @param id Node vertex id
     * @return Node
     */
    public Node getNodeById(Transaction tx, Long id) {
        Result node = tx.run("MATCH (vertex) WHERE ID(vertex) = $idVertex RETURN vertex",
                parameters("idVertex", id));
        if (node.hasNext()) {
            return node.single().get("vertex").asNode();
        } else return null;
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
        vertexProps.put(DatabaseStructure.NodeProperty.LAYER_NAME.t(), layerName);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("vertexProps", vertexProps);
        params.put("idRes", resource.id());
        params.put("transEnd", DatabaseStructure.EdgeProperty.TRAN_END.t());
        params.put("transEndVal", latestCommittedRevision);

        params.put("layerName", DatabaseStructure.NodeProperty.LAYER_NAME.t());
        params.put("layerNameValue", layerName);
        Result result = tx.run(String.format("MATCH (resource)-[r:%s]->(layer)" +
                        " WHERE r[$transEnd] >= $transEndVal AND ID(resource) = $idRes" +
                        " AND layer[$layerName]=$layerNameValue" +
                        " RETURN layer", DatabaseStructure.MantaRelationshipType.IN_LAYER.t()), params);
        return result.hasNext()? result.list(r -> r.get("layer").asNode()): Collections.emptyList();
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

        if (!RevisionUtils.isVertexInRevisionInterval(tx, rootVertex, walkthroughRevInterval)) {
            throw new IllegalStateException(
                    "The argument rootNode does not exist in the given revision " + newEndRevision);
        }
        Relationship controlRelationship = tx.run(String.format("MATCH (child)-[r:%s|%s]->(parent)" +
                " WHERE ID(child)=$idChild AND ID(parent)=$idParent" +
                " RETURN r", DatabaseStructure.MantaRelationshipType.HAS_PARENT.t(), DatabaseStructure.MantaRelationshipType.HAS_RESOURCE.t()),
                parameters("idChild", rootVertex.id(), "idParent", parentVertexId))
                .single().get("r").asRelationship();
        if (!RevisionUtils.isEdgeInRevisionInterval(controlRelationship, walkthroughRevInterval)) {
            throw new IllegalStateException(
                    "The argument rootNode does not exist in the given revision " + newEndRevision);
        }
        if (newEndRevision < controlRelationship.get(DatabaseStructure.EdgeProperty.TRAN_START.t()).asDouble()) {
            deleteSubtree(tx, rootVertex.id());
            return;
        }

        // Edge properties
        Map<String, Object> edgeProps = new HashMap<>();
        edgeProps.put(DatabaseStructure.EdgeProperty.TRAN_END.t(), newEndRevision);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idNode", rootVertex.id());
        params.put("edgeProps", edgeProps);
        params.put("vertexType", DatabaseStructure.NodeProperty.VERTEX_TYPE.t());
        params.put("vertexAttributeVal", MantaNodeLabel.ATTRIBUTE.toString());
        params.put("vertexNodeVal", MantaNodeLabel.NODE.toString());

         Result res = tx.run(String.format("MATCH path = (m)<-[:%s*0..]-(b)-[:%s*0..1]->(a)" +
                " WHERE id(m)=$idNode AND (nodes(path)[size(nodes(path))-1][$vertexType]=$vertexAttributeVal" +
                " OR (nodes(path)[size(nodes(path))-1][$vertexType]= $vertexNodeVal" +
                " AND NOT (a)<-[:%s]-() AND NOT (a)-[:%s]->()))" +
                " UNWIND nodes(path) as allNodes" +
                " MATCH (allNodes)-[rels]-()" +
                " SET rels += $edgeProps" +
                " return path",
                DatabaseStructure.MantaRelationshipType.HAS_PARENT.t(), DatabaseStructure.MantaRelationshipType.HAS_ATTRIBUTE.t(),
                DatabaseStructure.MantaRelationshipType.HAS_PARENT.t(), DatabaseStructure.MantaRelationshipType.HAS_ATTRIBUTE.t()), params);

        List<Record> records = res.hasNext() ? res.list(): Collections.emptyList();
        Set<Long> deletedNodes = new HashSet<>();
        for (Record record : records) {
            Value pathValue = record.get("path");
            if (pathValue != null) {
                Path path = pathValue.asPath();
                for (Relationship relationship : path.relationships()) {
                    if (deletedNodes.contains(relationship.startNodeId())
                            || deletedNodes.contains(relationship.endNodeId())) {
                        break;
                    }
                    if (!RevisionUtils.isEdgeInRevisionInterval(relationship, walkthroughRevInterval)) {
                        throw new IllegalStateException(
                                "The argument rootNode does not exist in the given revision " + newEndRevision);
                    }
                    if (newEndRevision < relationship.get(DatabaseStructure.EdgeProperty.TRAN_START.t()).asDouble()) {
                        long nodeIdToDelete = -1;
                        if (relationship.type().equals(DatabaseStructure.MantaRelationshipType.HAS_PARENT.t())) {
                            nodeIdToDelete = relationship.startNodeId();
                        } else if (relationship.type().equals(DatabaseStructure.MantaRelationshipType.HAS_ATTRIBUTE.t())) {
                            nodeIdToDelete = relationship.endNodeId();
                        }
                        deleteSubtree(tx, nodeIdToDelete);
                        deletedNodes.add(nodeIdToDelete);
                        break;
                    }
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
        params.put("vertexType", DatabaseStructure.NodeProperty.VERTEX_TYPE.t());
        params.put("vertexAttributeVal", MantaNodeLabel.ATTRIBUTE.toString());
        params.put("vertexNodeVal", MantaNodeLabel.NODE.toString());
        tx.run( String.format("MATCH path = (a)<-[:%s*0..1]-(b)-[:%s*0..]->(m {type:'node'})" +
                        " WHERE id(m)=$idNode AND (nodes(path)[0][$vertexType]=$vertexAttributeVal" +
                        " OR (nodes(path)[0][$vertexType]=$vertexNodeVal" +
                        " AND NOT (a)<-[:%s]-() AND NOT (a)-[:%s]->()))" +
                        " UNWIND nodes(path) as allNodes" +
                        " DETACH DELETE allNodes",
                DatabaseStructure.MantaRelationshipType.HAS_ATTRIBUTE.t(), DatabaseStructure.MantaRelationshipType.HAS_PARENT.t(),
                DatabaseStructure.MantaRelationshipType.HAS_PARENT.t(), DatabaseStructure.MantaRelationshipType.HAS_ATTRIBUTE.t()),
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
        Result result = tx.run(String.format("MATCH (node)-[:%s]->(parent)" +
                " WHERE ID(node)=$idNode" +
                " RETURN parent", DatabaseStructure.MantaRelationshipType.HAS_PARENT.t()), parameters("idNode", node.id()));
        return result.hasNext()? result.single().get("parent").asNode(): null;
    }

    /**
     * Gets vertex by qualified name
     * @param tx Database transaction
     * @param root Root vertex
     * @param qn List of qualified triplets (resource name, name, type)
     * @param revisionInterval Revision interval
     * @param repositoryPermissionProvider Permission provider
     * @return Node or null if node not found
     */
    public Node getVertexByQualifiedName(Transaction tx, Node root, List<List<String>> qn,
                                                RevisionInterval revisionInterval, RepositoryPermissionProvider repositoryPermissionProvider) {
        List<Node> vertexPath = getVertexPathFromQualifiedName(tx, root, qn, revisionInterval, repositoryPermissionProvider);
        if (vertexPath == null || vertexPath.isEmpty()) {
            return null;
        }
        return vertexPath.get(vertexPath.size() - 1);
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
     * @param repositoryPermissionProvider Permission provider
     * @return List of nodes on path
     */
    public List<Node> getVertexPathFromQualifiedName(Transaction tx, Node root, List<List<String>> qn,
                                                            RevisionInterval revisionInterval,
                                                            RepositoryPermissionProvider repositoryPermissionProvider) {
        if (qn == null || qn.isEmpty()) {
            return null;
        }

        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idSuperRoot", root.id());

        params.put("resNameProp", DatabaseStructure.NodeProperty.RESOURCE_NAME.t());
        params.put("resName", qn.get(0).get(2));

        params.put("transStartProp", DatabaseStructure.EdgeProperty.TRAN_START.t());
        params.put("transEndProp", DatabaseStructure.EdgeProperty.TRAN_END.t());
        params.put("tranStart", revisionInterval.getEnd());
        params.put("tranEnd", revisionInterval.getStart());

        params.put("nodeName", DatabaseStructure.NodeProperty.NODE_NAME.t());
        params.put("nodeType", DatabaseStructure.NodeProperty.NODE_TYPE.t());
        params.put("qn", qn);
        Result result = tx.run(String.format("WITH $qn AS path" +
                " MATCH p = (superRoot)<-[rel:%s]-(res)<-[*%d]-()" +
                " WHERE ID(superRoot) = $idSuperRoot AND toLower(res[$resNameProp])=toLower($resName)" +
                    " AND rel[$transStartProp] <= $tranStart" +
                    " AND rel[$transEndProp] >= $tranEnd" +
                    " AND ALL(idx in range(2, size(nodes(p))-1)" +
                        " WHERE toLower(coalesce(NODES(p)[idx][$nodeName], '')) = toLower(path[idx-1][0])" +
                            " AND toLower(coalesce(NODES(p)[idx][$nodeType], '')) = toLower(path[idx-1][1])" +
                            " AND relationships(p)[idx-1][$transStartProp] <= $tranStart" +
                            " AND relationships(p)[idx-1][$transEndProp] >= $tranEnd) " +
                " UNWIND nodes(p) AS n\n" +
                " MATCH resPaths = (n)-[:hasResource]->(r)" +
                " RETURN p, COLLECT(resPaths) as nodeToResourcePaths",
                DatabaseStructure.MantaRelationshipType.HAS_RESOURCE.t(), qn.size() - 1), params);

        List<Node> finalPath = new ArrayList<>();
        int k = 0;
        Map<Integer, Iterator<Node>> nodePaths = new HashMap<>();
        Map<Long, String> nodeToResourceName = new HashMap<>();
        Map<Integer, String> pathToResource = new HashMap<>();
        if (result.hasNext()) {
            for (Record record : result.list()) {
                Value pathValue = record.get("p");
                List<Path> nodeToResourcePaths = record.get("nodeToResourcePaths").asList(Value::asPath);
                nodeToResourcePaths.forEach(path -> nodeToResourceName
                        .put(path.start().id(), path.end().get(DatabaseStructure.NodeProperty.RESOURCE_NAME.t()).asString()));
                if (pathValue != null) {
                    Iterator<Node> it = pathValue.asPath().nodes().iterator();
                    it.next();
                    Node resource = it.next();
                    pathToResource.put(k, resource.get(DatabaseStructure.NodeProperty.RESOURCE_NAME.t()).asString());
                    nodePaths.put(k++, it);
                }
            }
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
                String examinedResourceName = nodeToResourceName.getOrDefault(examined.id(),
                        pathToResource.get(entry.getKey()));

                if (StringUtils.equals(examined.get(DatabaseStructure.NodeProperty.NODE_NAME.t()).asString(), name)
                        && (StringUtils.isEmpty(type) || StringUtils.equals(examined.get(DatabaseStructure.NodeProperty.NODE_TYPE.t()).asString(), type))
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
        Result result = tx.run(String.format("MATCH (node) WHERE ID(node)=$idNode" +
                        " WITH node" +
                        " OPTIONAL MATCH path=(resource:Resource)<-[:%s]-()<-[:%s*0..]-(node)" +
                        " RETURN path ORDER BY LENGTH(path) ASC LIMIT 1",
                DatabaseStructure.MantaRelationshipType.HAS_RESOURCE.t(),
                DatabaseStructure.MantaRelationshipType.HAS_PARENT.t()),
                parameters("idNode", nodeId));
        if (result.hasNext()) {
            List<Record> records = result.list();
            Record record = records.get(records.size() - 1);
            Value pathValue = record.get("path");
            if (pathValue != null) {
                Path path = pathValue.asPath();
                for (Node nodeInPath : path.nodes()) {
                    if (DriverMantaNodeLabelConverter.getType(nodeInPath) != MantaNodeLabel.ATTRIBUTE) {
                        nodePath.add(nodeInPath);
                    }
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
    public boolean isMantaRelationshipTypeCorrect(Node sourceVertex, Node targetVertex, DatabaseStructure.MantaRelationshipType label) {
        MantaNodeLabel sourceType = DriverMantaNodeLabelConverter.getType(sourceVertex);
        MantaNodeLabel targetType = DriverMantaNodeLabelConverter.getType(targetVertex);

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
     * Processes the value in case of {@link DatabaseStructure.EdgeProperty#CHILD_NAME}.
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
     * Processes the value in case of {@link DatabaseStructure.EdgeProperty#SOURCE_LOCAL_NAME}.
     *
     * @param localName String to process
     * @return Processed string.
     */
    public String processValueForLocalName(String localName) {
        return String.valueOf(localName.toLowerCase().hashCode());
    }
}
