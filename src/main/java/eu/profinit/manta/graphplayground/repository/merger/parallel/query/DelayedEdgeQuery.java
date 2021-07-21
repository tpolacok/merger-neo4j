package eu.profinit.manta.graphplayground.repository.merger.parallel.query;

import java.util.List;
import java.util.Map;

import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;
import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;

/**
 * Delayed edge query.
 * Represents creation of new edge.
 *
 * @author tpolacok
 */
public class DelayedEdgeQuery implements EdgeQuery {

    /**
     * Edge local id.
     */
    private final String localId;

    /**
     * Source node id.
     */
    private final Long sourceNodeId;

    /**
     * Target node id.
     */
    private final Long targetNodeId;

    /**
     * Relationship type.
     */
    private final DatabaseStructure.MantaRelationshipType edgeType;

    /**
     * Revision interval.
     */
    private final RevisionInterval revisionInterval;

    /**
     * Edge properties.
     */
    private final Map<String, Object> properties;

    /**
     * @param localId Edge local id.
     * @param sourceNodeId Source node id.
     * @param targetNodeId Target node id.
     * @param edgeType Edge type.
     * @param revisionInterval Revision interval.
     * @param properties Edge properties.
     */
    public DelayedEdgeQuery(String localId, Long sourceNodeId, Long targetNodeId,
                            DatabaseStructure.MantaRelationshipType edgeType, RevisionInterval revisionInterval,
                            Map<String, Object> properties) {
        this.localId = localId;
        this.sourceNodeId = sourceNodeId;
        this.targetNodeId = targetNodeId;
        this.edgeType = edgeType;
        this.revisionInterval = revisionInterval;
        this.properties = properties;
    }

    @Override
    public String identify() {
        return localId;
    }

    @Override
    public List<MergerProcessorResult> evaluate(StoredMergerProcessor mergerProcessor, StoredProcessorContext context) {
        mergerProcessor.mergeNewEdge(this, context);
        return List.of();
    }

    /**
     * @return Edge local id.
     */
    public String getLocalId() {
        return localId;
    }

    /**
     * @return Source node id.
     */
    public Long getSourceNodeId() {
        return sourceNodeId;
    }

    /**
     * @return Target node id.
     */
    public Long getTargetNodeId() {
        return targetNodeId;
    }

    /**
     * @return Edge type.
     */
    public DatabaseStructure.MantaRelationshipType getEdgeType() {
        return edgeType;
    }

    /**
     * @return Revision interval.
     */
    public RevisionInterval getRevisionInterval() {
        return revisionInterval;
    }

    /**
     * @return Edge properties.
     */
    public Map<String, Object> getProperties() {
        return properties;
    }
}
