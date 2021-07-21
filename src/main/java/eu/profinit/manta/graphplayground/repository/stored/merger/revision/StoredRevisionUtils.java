package eu.profinit.manta.graphplayground.repository.stored.merger.revision;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.model.manta.util.StoredMantaNodeLabelConverter;
import org.apache.commons.lang3.Validate;
import org.neo4j.graphdb.*;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper class for working with revisions.
 *
 * @author pholecek
 * @author tfechtner
 * @author tpolacok
 */
@Repository
public class StoredRevisionUtils {

    /**
     * Double in the Titan has maximum scale up to 6 decimal digits.
     */
    public static final Integer SCALE = 6;

    /**
     * The maximum minor revision number within one major revision.
     */
    public static final BigDecimal MINOR_REVISION_MAXIMUM_NUMBER = new BigDecimal("0.999999").setScale(SCALE,
            RoundingMode.DOWN);

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
    public void setVertexTransactionEnd(Transaction tx, Node vertex, Double newEndRevision) {
        Validate.notNull(vertex);
        Validate.notNull(newEndRevision);

        MantaNodeLabel vertexType = StoredMantaNodeLabelConverter.getType(vertex);
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
        params.put("idNode", vertex.getId());

        if (direction == Direction.INCOMING) {
            tx.execute( String.format("MATCH (node)<-[controlEdge:%s]-()" +
                    " WHERE ID(node)=$idNode" +
                    " SET controlEdge += $edgeProps", edgeLabel.toString()), params);
        } else {
            tx.execute( String.format("MATCH (node)-[controlEdge:%s]->()" +
                    " WHERE ID(node)=$idNode" +
                    " SET controlEdge += $edgeProps", edgeLabel.toString()), params);
        }
    }

    /**
     * Updates ending revision of the edge.
     * @param tx Graph transaction.
     * @param edge Edge to be updated.
     * @param newEndRevision New ending revision.
     */
    public void setEdgeTransactionEnd(Transaction tx, Relationship edge, Double newEndRevision) {
        Validate.notNull(edge);
        Validate.notNull(newEndRevision);

        Double currentTranStart = Double.parseDouble(edge.getProperty(DatabaseStructure.EdgeProperty.TRAN_START.t()).toString());
        if (newEndRevision < currentTranStart) {
            throw new IllegalStateException("Cannot set transaction end smaller than transaction start.");
        }

        // Edge properties
        Map<String, Object> edgeProps = new HashMap<>();
        edgeProps.put(DatabaseStructure.EdgeProperty.TRAN_END.t(), newEndRevision);
        // Parameters
        Map<String,Object> params = new HashMap<>();
        params.put("idRelation", edge.getId());
        params.put("edgeProps", edgeProps);
        tx.execute( "MATCH ()-[r]-() WHERE ID(r)=$idRelation SET r += $edgeProps RETURN r", params);
    }

    /**
     * Validates whether the given vertex belongs to given revision interval.
     * @param tx Graph transaction.
     * @param vertex Given vertex.
     * @param revInterval Given revision interval.
     * @return Boolean flag indicating whether the vertex belongs to given revision interval.
     */
    public boolean isVertexInRevisionInterval(Transaction tx, Node vertex, RevisionInterval revInterval) {
        Validate.notNull(vertex);
        Validate.notNull(revInterval);

        MantaNodeLabel vertexType = StoredMantaNodeLabelConverter.getType(vertex);
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
            result = tx.execute(String.format(
                    "MATCH (node)<-[controlEdge:%s]-()" +
                            "   WHERE ID(node)=$idNode\n" +
                            "RETURN controlEdge", edgeLabel),
                    Map.of("idNode", vertex.getId()));
        } else {
            result = tx.execute( String.format(
                    "MATCH (node)-[controlEdge:%s]->()" +
                            "   WHERE ID(node)=$idNode\n" +
                            "RETURN controlEdge", edgeLabel),
                    Map.of("idNode", vertex.getId()));
        }
        List<Relationship> edges = result.hasNext()
                ? result.stream()
                .map(entry -> (Relationship)entry.get("controlEdge"))
                .collect(Collectors.toList())
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
    public boolean isEdgeInRevisionInterval(Relationship edge, RevisionInterval revInterval) {
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
    public RevisionInterval getRevisionInterval(Relationship edge) {
        Validate.notNull(edge, "Edge must not be null.");

        Double edgeTranStart = (Double)edge.getProperty(DatabaseStructure.EdgeProperty.TRAN_START.t());
        Double edgeTranEnd = (Double)edge.getProperty(DatabaseStructure.EdgeProperty.TRAN_END.t());

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
