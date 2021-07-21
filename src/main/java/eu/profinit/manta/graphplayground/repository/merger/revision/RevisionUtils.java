package eu.profinit.manta.graphplayground.repository.merger.revision;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.model.manta.util.DriverMantaNodeLabelConverter;
import org.apache.commons.lang3.Validate;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.neo4j.graphdb.Direction;
import org.springframework.stereotype.Repository;

import eu.profinit.manta.graphplayground.repository.merger.connector.GraphCreation;

import static org.neo4j.driver.Values.parameters;

/**
 * Helper class for working with revisions.
 *
 * @author pholecek
 * @author tfechtner
 * @author tpolacok
 */
@Repository
public class RevisionUtils {

    /**
     * Double in the Titan has maximum scale up to 6 decimal digits.
     */
    public static final Integer SCALE = 6;

    /**
     * The maximum minor revision number within one major revision
     */
    public static final BigDecimal MINOR_REVISION_MAXIMUM_NUMBER = new BigDecimal("0.999999").setScale(SCALE,
            RoundingMode.DOWN);

    /**
     * Graph creation instance.
     */
    private final GraphCreation graphCreation;

    public RevisionUtils(GraphCreation graphCreation) {
        this.graphCreation = graphCreation;
    }

//    /**
//     * Metoda vyhledá pomocí indexu všechny hrany navázané na určitý uzel v rámci dané revize.
//     * @param vertex uzel, k němuž se uzly vyhledávají
//     * @param revInterval interval prohledávaných revizí
//     * @param direction směr hran
//     * @param edgeLabels typy hran, přes které se má hledat
//     * @return nalezené vrcholy
//     */
//    public static Iterable<Edge> getAdjacentEdges(Vertex vertex, Direction direction, RevisionInterval revInterval,
//                                                  RepositoryPermissionProvider repositoryPermissionProvider, String... edgeLabels) {
//        Validate.notNull(vertex);
//        Validate.notNull(revInterval);
//        Validate.notNull(direction);
//        Validate.notEmpty(edgeLabels);
//
//        Iterable<Edge> allAdjacentEdges = vertex.query().has(EdgeProperty.TRAN_START.t(), Cmp.LESS_THAN_EQUAL, revInterval.getEnd())
//                .has(EdgeProperty.TRAN_END.t(), Cmp.GREATER_THAN_EQUAL, revInterval.getStart()).labels(edgeLabels)
//                .direction(direction).edges();
//        if (repositoryPermissionProvider.hasEveryoneAllRepositoryPermission()) {
//            // pokud mame opravneni na cely repozitar, vratime vsechny navazane hrany
//            return allAdjacentEdges;
//        }
//        // jinak vratime pouze ty navazane hrany, k jejichz uzlum mame opravneni
//        List<Edge> adjacentEdges = new LinkedList<>();
//        for (Edge adjacentEdge: allAdjacentEdges) {
//            Vertex adjacentVertex = GraphOperation.getOppositeVertex(adjacentEdge, vertex, direction);
//            if (repositoryPermissionProvider.hasCurrentUserVertexPermission(adjacentVertex)) {
//                adjacentEdges.add(adjacentEdge);
//            }
//        }
//        return adjacentEdges;
//    }
//
//    /**
//     * Get all adjacent edges (with any label) of a vertex valid in a specific revision interval.
//     *
//     * @param vertex  vertex, to which the searched edges are connected
//     * @param revInterval  revision interval of the edges
//     * @param direction  direction of the edges
//     * @return edges of the vertex
//     */
//    public static Iterable<Edge> getAdjacentEdges(Vertex vertex, Direction direction, RevisionInterval revInterval) {
//        Validate.notNull(vertex);
//        Validate.notNull(revInterval);
//        Validate.notNull(direction);
//
//        return vertex.query().has(EdgeProperty.TRAN_START.t(), Cmp.LESS_THAN_EQUAL, revInterval.getEnd())
//                .has(EdgeProperty.TRAN_END.t(), Cmp.GREATER_THAN_EQUAL, revInterval.getStart()).direction(direction)
//                .edges();
//    }
//
//    /**
//     * Metoda vyhledá pomocí indexu všechny uzly navázané na určitý uzel v rámci dané revize.
//     * @param vertex uzel, k němuž se uzly vyhledávají
//     * @param revInterval interval prohledávaných revizí
//     * @param direction směr hran
//     * @param edgeLabels typy hran, přes které se má hledat
//     * @return nalezené vrcholy
//     */
//    public static Iterable<Vertex> getAdjacentVertices(Vertex vertex, RevisionInterval revInterval, Direction direction,
//                                                       RepositoryPermissionProvider repositoryPermissionProvider, String... edgeLabels) {
//        Validate.notNull(vertex);
//        Validate.notNull(revInterval);
//        Validate.notNull(direction);
//        Validate.notNull(repositoryPermissionProvider);
//        Validate.notEmpty(edgeLabels);
//
//        Iterable<Vertex> allAdjacentVertices = vertex.query().has(EdgeProperty.TRAN_START.t(), Cmp.LESS_THAN_EQUAL, revInterval.getEnd())
//                .has(EdgeProperty.TRAN_END.t(), Cmp.GREATER_THAN_EQUAL, revInterval.getStart()).labels(edgeLabels)
//                .direction(direction).vertices();
//        if (repositoryPermissionProvider.hasEveryoneAllRepositoryPermission()) {
//            // pokud mame opravneni na cely repozitar, vratime vsechny navazane uzly
//            return allAdjacentVertices;
//        }
//        // jinak vratime pouze ty navazane uzly, ke kterym mame opravneni
//        List<Vertex> adjacentVertices = new LinkedList<>();
//        for (Vertex adjacentVertex : allAdjacentVertices) {
//            if (repositoryPermissionProvider.hasCurrentUserVertexPermission(adjacentVertex)) {
//                adjacentVertices.add(adjacentVertex);
//            }
//        }
//        return adjacentVertices;
//    }
//
//    /**
//     * The method uses the index to search for all nodes bound to a specific node within a given revision.
//     *
//     * @param vertex      the node to which the nodes are searched.
//     * @param revInterval interval of searched revisions.
//     * @param direction   direction of edges.
//     * @param edgeLabels  types of edges to search through.
//     * @param selectedLayer layer for aggregated lineage to be checked with other perspective edges.
//     * @return found vertices
//     */
//    public static Iterable<Vertex> getAdjacentVertices(String selectedLayer, Vertex vertex, RevisionInterval revInterval, Direction direction,
//                                                       RepositoryPermissionProvider repositoryPermissionProvider, String... edgeLabels) {
//        Validate.notNull(vertex);
//        Validate.notNull(revInterval);
//        Validate.notNull(direction);
//        Validate.notNull(repositoryPermissionProvider);
//        Validate.notEmpty(edgeLabels);
//
//        Iterable<Vertex> allAdjacentVertices = vertex.query().has(EdgeProperty.TRAN_START.t(), Cmp.LESS_THAN_EQUAL, revInterval.getEnd())
//                .has(EdgeProperty.TRAN_END.t(), Cmp.GREATER_THAN_EQUAL, revInterval.getStart()).labels(edgeLabels).has(EdgeProperty.LAYER.t(), Cmp.EQUAL, selectedLayer)
//                .direction(direction).vertices();
//        if (repositoryPermissionProvider.hasEveryoneAllRepositoryPermission()) {
//            return allAdjacentVertices;
//        }
//        List<Vertex> adjacentVertices = new LinkedList<>();
//        for (Vertex adjacentVertex : allAdjacentVertices) {
//            if (repositoryPermissionProvider.hasCurrentUserVertexPermission(adjacentVertex)) {
//                adjacentVertices.add(adjacentVertex);
//            }
//        }
//        return adjacentVertices;
//    }

    /**
     * Set end revision of the vertex by setting tranEnd of its control edge(s).
     * Every vertex has always a unique set of control edges valid in one and only one revision interval.
     * In other words, no vertex can have more control edges in different revision intervals.
     *
     * End revision can be set to any value (increased and decreased by almost any value).
     * Thus, there is no constraint or condition for setting a new tranEnd number based on the current tranEnd number (see examples below).
     *
     * Example 1: tranEnd can be increased from 1.999999 to 2.999999
     * (when major revision 2.000000 was created and the vertex is present in this new major revision as well)
     *
     * Example 2: tranEnd can be decreased from 1.999999 to 1.000001
     * (when the vertex was removed in the revision 1.000002)
     *
     * Example 3: tranEnd can be decreased from 2.999999 to 1.999999
     * (very special case when dealing with the overflow of minor revisions in the major revision 1)
     *
     * @param tx Database transaction
     * @param vertex  vertex, whose control edge(s) will have set a new end revision
     * @param newEndRevision  new value of the end revision number
     */
    public static void setVertexTransactionEnd(Transaction tx, Node vertex, Double newEndRevision) {
        Validate.notNull(vertex);
        Validate.notNull(newEndRevision);

        MantaNodeLabel vertexType = DriverMantaNodeLabelConverter.getType(vertex);
        Direction direction = vertexType.getControlEdgeDirection();
        String[] edgeLabels = vertexType.getControlEdgeLabels();

        StringBuilder edgeLabel = new StringBuilder(edgeLabels[0]);
        for (int i = 1; i < edgeLabels.length; ++i) {
            edgeLabel.append("|").append(edgeLabels[i]);
        }
        // Edge properties
        Map<String, Object> edgeProps = new HashMap<>();
        edgeProps.put(DatabaseStructure.EdgeProperty.TRAN_END.t(), newEndRevision);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("edgeProps", edgeProps);
        params.put("idNode", vertex.id());
        if (direction == Direction.INCOMING) {
            tx.run( String.format("MATCH (node)<-[controlEdge:%s]-()" +
                    " WHERE ID(node)=$idNode" +
                    " SET controlEdge += $edgeProps", edgeLabel.toString()), params);
        } else {
            tx.run( String.format("MATCH (node)-[controlEdge:%s]->()" +
                    " WHERE ID(node)=$idNode" +
                    " SET controlEdge += $edgeProps", edgeLabel.toString()), params);
        }
    }

    /**
     * Sets the ending revision for the edge.
     * @param tx Database transaction.
     * @param edge Given edge.
     * @param newEndRevision New ending revision.
     */
    public void setEdgeTransactionEnd(Transaction tx, Relationship edge, Double newEndRevision) {
        Validate.notNull(edge);
        Validate.notNull(newEndRevision);

        Double currentTranStart = edge.get(DatabaseStructure.EdgeProperty.TRAN_START.t()).asDouble();
        if (newEndRevision < currentTranStart) {
            throw new IllegalStateException("Cannot set transaction end smaller than transaction start.");
        }
        graphCreation.addRelationshipProperty(tx, edge, DatabaseStructure.EdgeProperty.TRAN_END.t(), newEndRevision);
    }
//
//    /**
//     * Nastaví dané hraně transakční start.
//     * @param edge hrana, která se má nastavit
//     * @param newStartRevision číslo nové revize, na které se bude transakční začátek nastavovat
//     */
//    public static void setEdgeTransactionStart(Edge edge, Double newStartRevision) {
//        Validate.notNull(edge);
//        Validate.notNull(newStartRevision);
//
//        Integer currentTranEnd = edge.getProperty(EdgeProperty.TRAN_END.t());
//        if (newStartRevision > currentTranEnd) {
//            throw new IllegalStateException("Cannot set transaction start greater than transaction end.");
//        }
//        edge.setProperty(EdgeProperty.TRAN_START.t(), newStartRevision);
//    }

    /**
     * Validates whether the given vertex belongs to given revision interval.
     * @param tx Graph transaction.
     * @param vertex Given vertex.
     * @param revInterval Given revision interval.
     * @return Boolean flag indicating whether the vertex belongs to given revision interval.
     */
    public static boolean isVertexInRevisionInterval(Transaction tx, Node vertex, RevisionInterval revInterval) {
        Validate.notNull(vertex);
        Validate.notNull(revInterval);

        MantaNodeLabel vertexType = DriverMantaNodeLabelConverter.getType(vertex);
        Direction direction = vertexType.getControlEdgeDirection();
        if (direction == null) {
            // special nodes that are in all revisions
            return true;
        }

        String[] edgeLabels = vertexType.getControlEdgeLabels();

        StringBuilder edgeLabel = new StringBuilder(edgeLabels[0]);
        for (int i = 1; i < edgeLabels.length; ++i) {
            edgeLabel.append("|").append(edgeLabels[i]);
        }
        Result result = null;
        if (direction == Direction.INCOMING) {
            result = tx.run(String.format(
                        "MATCH (node)<-[controlEdge:%s]-()" +
                           "   WHERE ID(node)=$idNode\n" +
                           "RETURN controlEdge", edgeLabel),
                    parameters("idNode", vertex.id()));
        } else {
            result = tx.run( String.format(
                        "MATCH (node)-[controlEdge:%s]->()" +
                        "   WHERE ID(node)=$idNode\n" +
                        "RETURN controlEdge", edgeLabel.toString()),
                    parameters("idNode", vertex.id()));
        }
        List<Relationship> edges = result.hasNext() ? result.list(r -> r.get("controlEdge").asRelationship())
                : Collections.emptyList();

        boolean allEdgesValid = true;
        for (Relationship edge: edges) allEdgesValid &= isEdgeInRevisionInterval(edge, revInterval);
        return allEdgesValid;
    }

    /**
     * Validates whether the given edge belongs to given revision interval.
     * @param edge Given edge.
     * @param revInterval Given revision interval.
     * @return Boolean flag indicating whether the edge belongs to given revision interval.
     */
    public static boolean isEdgeInRevisionInterval(Relationship edge, RevisionInterval revInterval) {
        Validate.notNull(edge);
        Validate.notNull(revInterval);

        RevisionInterval edgeRevInterval = getRevisionInterval(edge);
        boolean isEdgeValid = ((revInterval.getStart() <= edgeRevInterval.getEnd())
                && (revInterval.getEnd() >= edgeRevInterval.getStart()));
        return isEdgeValid;
    }

    /**
     * Return revision interval of the edge.
     * @param edge Given edge.
     * @return Revision interval of the given edge.
     * @throws IllegalStateException Edge has no revision properties.
     */
    public static RevisionInterval getRevisionInterval(Relationship edge) {
        Validate.notNull(edge, "Edge must not be null.");

        Double edgeTranStart = edge.get(DatabaseStructure.EdgeProperty.TRAN_START.t()).asDouble();
        Double edgeTranEnd = edge.get(DatabaseStructure.EdgeProperty.TRAN_END.t()).asDouble();

        if (edgeTranStart == null || edgeTranEnd == null) {
            throw new IllegalStateException("Edge doesn't have all transaction data.");
        }

        return new RevisionInterval(edgeTranStart, edgeTranEnd);
    }

    /**
     * Get maximum minor revision number from a specified revision number. That means, get "majorRevision.999999".
     * <p>
     * For instance, revision = 1.123456. Then, the output is "1.999999".
     *
     * @param revision  revision number
     * @return maximum minor revision number
     */
    public static Double getMaxMinorRevisionNumber(Double revision) {
        BigDecimal integerPart = BigDecimal.valueOf(Math.floor(revision)).setScale(SCALE, RoundingMode.DOWN);
        BigDecimal maximumMinorRevision = integerPart.add(MINOR_REVISION_MAXIMUM_NUMBER);
        return maximumMinorRevision.doubleValue();
    }
}
