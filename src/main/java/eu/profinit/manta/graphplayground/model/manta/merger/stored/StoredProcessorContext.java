package eu.profinit.manta.graphplayground.model.manta.merger.stored;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.profinit.manta.graphplayground.model.manta.merger.AbstractProcessorContext;
import eu.profinit.manta.graphplayground.model.manta.merger.SpecificDataHolder;
import org.neo4j.graphdb.Transaction;

import eu.profinit.manta.graphplayground.model.manta.core.EdgeIdentification;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;

/**
 * Context for stored procedure processing.
 * Defines database transaction object.
 *
 * @author tpolacok
 */
public class StoredProcessorContext extends AbstractProcessorContext {

    /**
     * Mapping of edge's local identifier to its attributes' definitions
     */
    private Map<String, List<CommonMergeQuery>> edgeToAttributes = new HashMap<>();

    /**
     * List of node attribute queries.
     */
    protected List<CommonMergeQuery> nodeAttributeQueryList = Collections.synchronizedList(new ArrayList<>());

    /**
     * List of edge queries.
     */
    protected List<CommonMergeQuery> edgeQueryList = Collections.synchronizedList(new ArrayList<>());

    /**
     * Transaction access object.
     */
    protected Transaction dbTransaction;

    /**
     * @param dbTransaction Database transactional object.
     * @param mapNodeIdToDbId Mapping of local node id to its database id.
     * @param mapResourceIdToDbId Mapping of local resource id to its database id.
     * @param mapLayerIdToLayer Mapping of local layer id to its merging instruction.
     * @param mapEdgeIdToDbId Mapping of local edge id to its database id.
     * @param mapDbIdToEdgeIds Mapping of edge database id to its local identifiers - //TODO required in Titan but not in neo4j
     * @param mapSourceCodeIdToDbId Mapping of local source code id to its database id.
     * @param specificDataHolder Specific data holder for requests.
     * @param revision Merged revision.
     * @param latestCommittedRevision Latest committed revision.
     * @param mapNodeDbIdResourceDbId Mapping of node database id to its resource database id.
     * @param nodesExistedBefore Set of node ids for nodes which existed prior to merging
     */
    public StoredProcessorContext(Transaction dbTransaction, Map<String, Long> mapNodeIdToDbId,
                                  Map<String, Long> mapResourceIdToDbId, Map<String, String[]> mapLayerIdToLayer,
                                  Map<String, EdgeIdentification> mapEdgeIdToDbId,
                                  Map<Long, Set<String>> mapDbIdToEdgeIds,
                                  Map<String, String> mapSourceCodeIdToDbId, SpecificDataHolder specificDataHolder,
                                  double revision, Double latestCommittedRevision,
                                  Map<Long, Long> mapNodeDbIdResourceDbId, Set<Long> nodesExistedBefore) {
        super(mapNodeIdToDbId, mapResourceIdToDbId, mapLayerIdToLayer, mapEdgeIdToDbId,
                mapDbIdToEdgeIds, mapSourceCodeIdToDbId, specificDataHolder, revision, latestCommittedRevision,
                mapNodeDbIdResourceDbId, nodesExistedBefore);
        this.dbTransaction = dbTransaction;
    }

    /**
     * Initializes empty context.
     */
    public StoredProcessorContext() {
        super();
        dbTransaction = null;
    }

    /**
     * Initializes context with revisions
     * @param revision Merged revision.
     * @param latestCommittedRevision Latest committed revision.
     */
    public StoredProcessorContext(Double revision, Double latestCommittedRevision) {
        super();
        this.revision = revision;
        this.latestCommittedRevision = latestCommittedRevision;
    }

    /**
     * @param edgeToAttributes Mapping of edge identifier to its attributes.
     */
    public void setEdgeToEdgeAttributeMap(Map<String, List<CommonMergeQuery>> edgeToAttributes) {
        this.edgeToAttributes.putAll(edgeToAttributes);
    }

    /**
     * @param localId Edge's local identifier.
     * @return List of attributes' definiton for certain edge local identifier.
     */
    public List<CommonMergeQuery> getEdgeAttributesForEdge(String localId) {
        return edgeToAttributes.getOrDefault(localId, Collections.emptyList());
    }

    /**
     * @return List of node attribute queries.
     */
    public List<CommonMergeQuery> getNodeAttributeQueryList() {
        return nodeAttributeQueryList;
    }

    /**
     * @return List of edge queries.
     */
    public List<CommonMergeQuery> getEdgeQueryList() {
        return edgeQueryList;
    }

    /**
     * Adds transaction object to context.
     * @param tx Database transaction object.
     */
    public void setServerDbTransaction(Transaction tx) {
        dbTransaction = tx;
    }

    /**
     * @return Transakční objekt do databáze.
     */
    public Transaction getServerDbTransaction() {
        return dbTransaction;
    }




}
