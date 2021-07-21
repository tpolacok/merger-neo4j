package eu.profinit.manta.graphplayground.repository.merger.parallel.processor;

import java.util.Map;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;

import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedEdgeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedNodeAttributeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedPerspectiveEdgeQuery;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphCreation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphOperation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSourceRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSuperRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionUtils;

/**
 * Modified merger processor to not perform creational queries.
 * Queries are stored in the context and later evaluated.
 *
 * @author tpolacok
 */
public class DelayedParallelStoredMergerProcessor extends ParallelStoredMergerProcessor {
    public DelayedParallelStoredMergerProcessor(StoredSuperRootHandler superRootHandler, StoredSourceRootHandler sourceRootHandler, StoredRevisionRootHandler revisionRootHandler, StoredGraphOperation graphOperation, StoredGraphCreation graphCreation, StoredRevisionUtils revisionUtils, Log logger) {
        super(superRootHandler, sourceRootHandler, revisionRootHandler, graphOperation, graphCreation, revisionUtils, logger);
    }

    @Override
    protected void createNodeAttribute(StoredProcessorContext context, Long nodeId, String key, Object value, RevisionInterval newRevisionInterval) {
        context.getNodeAttributeQueryList().add(new DelayedNodeAttributeQuery(nodeId, key, value, newRevisionInterval));
    }

    @Override
    protected void addMapsToNodeAttribute(StoredProcessorContext context, Long nodeId, Long mappedId, RevisionInterval newRevisionInterval) {
        context.getEdgeQueryList().add(new DelayedEdgeQuery("", nodeId, mappedId, DatabaseStructure.MantaRelationshipType.MAPS_TO, newRevisionInterval, Map.of()));
    }

    @Override
    protected Relationship createEdge(StoredProcessorContext context, String edgeId,
                                      Long sourceNodeId, Long targetNodeId, DatabaseStructure.MantaRelationshipType type,
                                      RevisionInterval revisionInterval) {
        context.getEdgeQueryList().add(new DelayedEdgeQuery(edgeId, sourceNodeId, targetNodeId,
                type, revisionInterval, Map.of()));
        return null;
    }

    @Override
    protected void createPerspectiveEdge(StoredProcessorContext context, Node layer, String edgeId,
                              Long sourceNodeId, Long targetNodeId, DatabaseStructure.MantaRelationshipType type,
                              RevisionInterval revisionInterval) {
        String key = graphOperation.getName(layer) + " perspective";
        Object value = graphOperation.getVertexPathString(context.getServerDbTransaction(), targetNodeId);
        context.getEdgeQueryList().add(new DelayedPerspectiveEdgeQuery(edgeId, sourceNodeId, targetNodeId,
                type, revisionInterval, Map.of(DatabaseStructure.EdgeProperty.LAYER.t(), value),
                new DelayedNodeAttributeQuery(sourceNodeId, key, value, revisionInterval)));
    }

    @Override
    public void copyEdge(StoredProcessorContext context, Relationship relationship,
                         RevisionInterval revisionInterval, Map<String, Object> attributes, String relationshipId) {
        context.getEdgeQueryList().add(
                new DelayedEdgeQuery(
                        relationshipId,
                        relationship.getStartNodeId(),
                        relationship.getEndNodeId(),
                        DatabaseStructure.MantaRelationshipType.parseFromDbType(relationship.getType().name()),
                        revisionInterval,
                        attributes
                )
        );

    }

}
