package eu.profinit.manta.graphplayground.model.manta.merger.parallel.context;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;

/**
 * Concurrent processor aimed for concurrent access to storing structures
 *
 * @author tpolacok
 */
public class ParallelStoredProcessorContext extends StoredProcessorContext {

    /**
     * Initializes context over shared main context.
     * @param other Other instance of {@link StoredProcessorContext} - all structures are shared.
     */
    public ParallelStoredProcessorContext(StoredProcessorContext other) {
        this.mapNodeIdToDbId = other.getMapNodeIdToDbId();
        this.mapResourceIdToDbId = other.getMapResourceIdToDbId();
        this.mapLayerIdToLayer = other.getMapLayerIdToLayer();
        this.mapEdgeIdToDbId = other.getMapEdgeIdToDbId();
        this.mapDbIdToEdgeIds = other.getMapDbIdToEdgeIds();
        this.mapSourceCodeIdToDbId = other.getMapSourceCodeIdToDbId();
        this.revision = other.getRevision();
        this.latestCommittedRevision = other.getLatestCommittedRevision();
        this.mapNodeDbIdResourceDbId = other.getMapNodeDbIdResourceDbId();
        this.nodesExistedBefore = other.getNodesExistedBefore();
        this.nodeAttributeQueryList = other.getNodeAttributeQueryList();
        this.edgeQueryList = other.getEdgeQueryList();
        this.specificDataHolder = null;
        this.dbTransaction = null;
    }

    /**
     * Initializes context with revisions
     * @param revision Merged revision.
     * @param latestCommittedRevision Latest committed revision.
     */
    public ParallelStoredProcessorContext(Double revision, Double latestCommittedRevision) {
        super();
        this.revision = revision;
        this.latestCommittedRevision = latestCommittedRevision;
    }

    @Override
    public void init() {
        this.mapNodeIdToDbId = new ConcurrentHashMap<>();
        this.mapResourceIdToDbId = new ConcurrentHashMap<>();
        this.mapLayerIdToLayer = new ConcurrentHashMap<>();
        this.mapEdgeIdToDbId = new ConcurrentHashMap<>();
        this.mapDbIdToEdgeIds = new ConcurrentHashMap<>();
        this.mapSourceCodeIdToDbId = new ConcurrentHashMap<>();
        this.specificDataHolder = null;
        this.revision = 0.0;
        this.latestCommittedRevision = 0.0;
        this.mapNodeDbIdResourceDbId = new ConcurrentHashMap<>();
        this.nodesExistedBefore = ConcurrentHashMap.newKeySet();
        this.nodeAttributeQueryList = Collections.synchronizedList(new ArrayList<>());
        this.edgeQueryList = Collections.synchronizedList(new ArrayList<>());
    }
}