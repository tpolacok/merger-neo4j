package eu.profinit.manta.graphplayground.repository.merger.parallel.query;

import java.util.List;

import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;

/**
 * Represents delayed node attribute creation query.
 *
 * @author tpolacok.
 */
public class DelayedNodeAttributeQuery implements CommonMergeQuery {

    /**
     * Node id.
     */
    private final Long nodeId;

    /**
     * Attribute key.
     */
    private final String attributeKey;

    /**
     * Attribute value.
     */
    private final Object attributeValue;

    /**
     * Revision interval.
     */
    private final RevisionInterval revisionInterval;

    /**
     * @param nodeId Node id.
     * @param attributeKey Attribute key.
     * @param attributeValue Attribute value.
     * @param revisionInterval Revision interval.
     */
    public DelayedNodeAttributeQuery(Long nodeId, String attributeKey, Object attributeValue, RevisionInterval revisionInterval) {
        this.nodeId = nodeId;
        this.attributeKey = attributeKey;
        this.attributeValue = attributeValue;
        this.revisionInterval = revisionInterval;
    }

    @Override
    public List<MergerProcessorResult> evaluate(StoredMergerProcessor mergerProcessor, StoredProcessorContext context) {
        mergerProcessor.mergeNewAttribute(this, context);
        return List.of();
    }

    /**
     * @return Node id.
     */
    public Long getNodeId() {
        return nodeId;
    }

    /**
     * @return Attribute key.
     */
    public String getAttributeKey() {
        return attributeKey;
    }

    /**
     * @return Attribute value.
     */
    public Object getAttributeValue() {
        return attributeValue;
    }

    /**
     * @return Revision interval.
     */
    public RevisionInterval getRevisionInterval() {
        return revisionInterval;
    }
}
