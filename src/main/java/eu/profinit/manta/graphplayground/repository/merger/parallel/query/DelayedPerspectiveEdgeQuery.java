package eu.profinit.manta.graphplayground.repository.merger.parallel.query;

import java.util.List;
import java.util.Map;

import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;
import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;

/**
 * Extension to {@link DelayedEdgeQuery} by adding node attribute query.
 */
public class DelayedPerspectiveEdgeQuery extends DelayedEdgeQuery {

    /**
     * Node attribute.
     */
    private final DelayedNodeAttributeQuery nodeAttributeQuery;

    /**
     * @param localId Edge local id.
     * @param sourceNodeId Source node id.
     * @param targetNodeId Target node id.
     * @param edgeType Edge type.
     * @param revisionInterval Revision interval.
     * @param nodeAttributeQuery Node attribute query.
     * @param properties Edge properties.
     */
    public DelayedPerspectiveEdgeQuery(String localId, Long sourceNodeId, Long targetNodeId,
                                       DatabaseStructure.MantaRelationshipType edgeType, RevisionInterval revisionInterval,
                                       Map<String, Object> properties, DelayedNodeAttributeQuery nodeAttributeQuery) {
        super(localId, sourceNodeId, targetNodeId, edgeType, revisionInterval, properties);
        this.nodeAttributeQuery = nodeAttributeQuery;
    }

    @Override
    public List<MergerProcessorResult> evaluate(StoredMergerProcessor mergerProcessor, StoredProcessorContext context) {
        mergerProcessor.mergePerspectiveEdge(this, context);
        return List.of();
    }

    /**
     * @return Node attribute query.
     */
    public DelayedNodeAttributeQuery getNodeAttributeQuery() {
        return nodeAttributeQuery;
    }
}
