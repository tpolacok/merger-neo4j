package eu.profinit.manta.graphplayground.model.manta.merger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import eu.profinit.manta.graphplayground.model.manta.core.EdgeIdentification;

/**
 * Abstract processor context
 * Declares caching and mapping structures for local to database objects representations.
 * Defines default initialization method.
 *
 * @author tpolacok
 */
public abstract class AbstractProcessorContext implements ProcessorContext {

    /**
     * Mapping of local node id to its database id.
     */
    protected Map<String, Long> mapNodeIdToDbId;

    /**
     * Mapping of local resource id to its database id.
     */
    protected Map<String, Long> mapResourceIdToDbId;

    /**
     * Mapping of local layer id to its database id.
     */
    protected Map<String, String[]> mapLayerIdToLayer;

    /**
     * Mapping of local edge id to its database id.
     */
    protected Map<String, EdgeIdentification> mapEdgeIdToDbId;

    /**
     * Mapping od database id to its local identifiers - //TODO required in Titan but not in neo4j
     */
    protected Map<Long, Set<String>> mapDbIdToEdgeIds;

    /**
     * Mapping of local source code id to its database id.
     */
    protected Map<String, String> mapSourceCodeIdToDbId;

    /**
     * Set of requested source codes.
     */
    protected Set<SourceCodeMapping> requestedSourceCodes = new HashSet<>();

    /**
     * Specific data holder for requests.
     */
    protected SpecificDataHolder specificDataHolder;

    /**
     * Merged revision.
     */
    protected double revision;

    /**
     * Latest committed revision number.
     */
    protected Double latestCommittedRevision;

    /**
     * Mapping of node database ids to their resource database ids.
     */
    protected Map<Long, Long> mapNodeDbIdResourceDbId;

    /**
     * Set of nodes which existed before.
     */
    protected Set<Long> nodesExistedBefore;

    /**
     * @param mapNodeIdToDbId Mapping of local node id to its database id.
     * @param mapResourceIdToDbId Mapping of local resource id to its database id.
     * @param mapLayerIdToLayer Mapping of local layer id to its merging instructions.
     * @param mapEdgeIdToDbId Mapping of local edge id to its database id.
     * @param mapDbIdToEdgeIds Mapping od database id to its local identifiers - //TODO required in Titan but not in neo4j
     * @param mapSourceCodeIdToDbId Mapping of local source code id to its database id.
     * @param specificDataHolder Specific data holder for requests.
     * @param revision Merged revision.
     * @param latestCommittedRevision Latest committed revision.
     * @param mapNodeDbIdResourceDbId Mapping of node database id to its resource database id.
     * @param nodesExistedBefore Set of node ids for nodes which existed prior to merging
     */
    public AbstractProcessorContext(Map<String, Long> mapNodeIdToDbId,
                                    Map<String, Long> mapResourceIdToDbId, Map<String, String[]> mapLayerIdToLayer,
                                    Map<String, EdgeIdentification> mapEdgeIdToDbId,
                                    Map<Long, Set<String>> mapDbIdToEdgeIds,
                                    Map<String, String> mapSourceCodeIdToDbId, SpecificDataHolder specificDataHolder,
                                    double revision, Double latestCommittedRevision,
                                    Map<Long, Long> mapNodeDbIdResourceDbId, Set<Long> nodesExistedBefore) {
        super();
        this.mapNodeIdToDbId = mapNodeIdToDbId;
        this.mapResourceIdToDbId = mapResourceIdToDbId;
        this.mapLayerIdToLayer = mapLayerIdToLayer;
        this.mapEdgeIdToDbId = mapEdgeIdToDbId;
        this.mapDbIdToEdgeIds = mapDbIdToEdgeIds;
        this.mapSourceCodeIdToDbId = mapSourceCodeIdToDbId;
        this.specificDataHolder = specificDataHolder;
        this.revision = revision;
        this.mapNodeDbIdResourceDbId = mapNodeDbIdResourceDbId;
        this.nodesExistedBefore = nodesExistedBefore;
        this.latestCommittedRevision = latestCommittedRevision;
    }

    public AbstractProcessorContext() {
        super();
        init();
    }

    /**
     * Initializes the internal structures.
     */
    public void init() {
        this.mapNodeIdToDbId = new HashMap<>();
        this.mapResourceIdToDbId = new HashMap<>();
        this.mapLayerIdToLayer = new HashMap<>();
        this.mapEdgeIdToDbId = new HashMap<>();
        this.mapDbIdToEdgeIds = new HashMap<>();
        this.mapSourceCodeIdToDbId = new HashMap<>();
        specificDataHolder = null;
        revision = 0.0;
        latestCommittedRevision = 0.0;
        this.mapNodeDbIdResourceDbId = new HashMap<>();
        this.nodesExistedBefore = new HashSet<>();
    }

    /**
     * @param mapNodeIdToDbId Mapping of local node id to its database id.
     * @param mapResourceIdToDbId Mapping of local resource id to its database id.
     * @param mapLayerIdToLayer Mapping of local layer id to its database id.
     * @param mapEdgeIdToDbId Mapping of local edge id to its database id.
     * @param mapDbIdToEdgeIds Mapping of edge database id to its local identifiers - //TODO required in Titan but not in neo4j
     * @param mapSourceCodeIdToDbId Mapping of local source code id to its database id.
     * @param specificDataHolder Specific data holder for requests.
     * @param revision Merged revision.
     * @param latestCommittedRevision Latest committed revision.
     * @param mapNodeDbIdResourceDbId Mapping of node database id to its resource database id.
     * @param nodesExistedBefore Set of node ids for nodes which existed prior to merging
     */

    /**
     * @return Mapping of local node id to its database id.
     */
    public Map<String, Long> getMapNodeIdToDbId() {
        return mapNodeIdToDbId;
    }

    /**
     * @return Mapping of local resource id to its database id.
     */
    public Map<String, Long> getMapResourceIdToDbId() {
        return mapResourceIdToDbId;
    }

    /**
     * @return Mapping of local source code id to its database id.
     */
    public Map<String, String> getMapSourceCodeIdToDbId() {
        return mapSourceCodeIdToDbId;
    }

    /**
     * @return Specific data holder for requests.
     */
    public SpecificDataHolder getSpecificDataHolder() {
        return specificDataHolder;
    }

    /**
     * @return Merged revision.
     */
    public double getRevision() {
        return revision;
    }

    /**
     * @return Latest committed revision number.
     */
    public Double getLatestCommittedRevision() {
        return latestCommittedRevision;
    }

    /**
     * @param sourceCodeMapping Add new source code.
     */
    public void addRequestedSourceCode(SourceCodeMapping sourceCodeMapping) {
        requestedSourceCodes.add(sourceCodeMapping);
    }

    /**
     * @return Set of requested source codes.
     */
    public Set<SourceCodeMapping> getRequestedSourceCodes() {
        return requestedSourceCodes;
    }

    /**
     * @return Mapping of local layer id to its merging instructions.
     */
    public Map<String, String[]> getMapLayerIdToLayer() {
        return mapLayerIdToLayer;
    }

    /**
     * @return Mapping of local edge id to its database id.
     */
    public Map<String, EdgeIdentification> getMapEdgeIdToDbId() {
        return mapEdgeIdToDbId;
    }

    /**
     * @return Mapping of edge database id to its local identifiers
     */
    public Map<Long, Set<String>> getMapDbIdToEdgeIds() {
        return mapDbIdToEdgeIds;
    }

    /**
     * Adds mapping of MANTA edge ID to database edge ID and vice versa.
     * @param mantaId MANTA edge ID
     */
    public void mapEdgeId(String mantaId, EdgeIdentification dbId) {
        mapEdgeIdToDbId.put(mantaId, dbId);
        // Relation ID is not part of EdgeIdentification equals method, so must be compared separately
        mapDbIdToEdgeIds.computeIfAbsent(dbId.getRelationId(), key -> new HashSet<>()).add(mantaId);
    }

    /**
     * Returns edge database ID corresponding to the MANTA ID.
     * @param edgeMantaId The MANTA edge ID
     * @return Edge database ID corresponding to the MANTA ID.
     */
    public EdgeIdentification getEdgeDbId(String edgeMantaId) {
        return mapEdgeIdToDbId.get(edgeMantaId);
    }

    /**
     * @return Return mappng of node db id to its resource db id.
     */
    public Map<Long, Long> getMapNodeDbIdResourceDbId() {
        return mapNodeDbIdResourceDbId;
    }

    /**
     * @return Set of nodes which existed before being merged.
     */
    public Set<Long> getNodesExistedBefore() {
        return nodesExistedBefore;
    }

    /**
     * Sets new revision.
     * @param revision New revision.
     */
    public void setRevision(double revision) {
        this.revision = revision;
    }

    /**
     * Sets latest committed revision.
     * @param latestCommittedRevision New latest committed revision.
     */
    public void setLatestCommittedRevision(Double latestCommittedRevision) {
        this.latestCommittedRevision = latestCommittedRevision;
    }

}
