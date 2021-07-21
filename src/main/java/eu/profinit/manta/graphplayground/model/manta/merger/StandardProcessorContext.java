package eu.profinit.manta.graphplayground.model.manta.merger;

import java.util.Map;
import java.util.Set;

import org.neo4j.driver.Transaction;

import eu.profinit.manta.graphplayground.model.manta.core.EdgeIdentification;

/**
 * Context for merging processor used for caching keeping mapping information of local objects
 * to its database counterparts while merging.
 *
 * @author tfechtner
 */
public class StandardProcessorContext extends AbstractProcessorContext{

    /**
     * Database transactional object.
     */
    private Transaction dbTransaction;

    /**
     * @param dbTransaction Database transactional object.
     * @param mapNodeIdToDbId Mapping of local node id to its database id.
     * @param mapResourceIdToDbId Mapping of local resource id to its database id.
     * @param mapLayerIdToLayer Mapping of local layer id to its merging instructions.
     * @param mapEdgeIdToDbId Mapping of local edge id to its database id.
     * @param mapDbIdToEdgeIds Mapping of edge database id to its local identifiers - //TODO required in Titan but not in neo4j
     * @param mapSourceCodeIdToDbId Mapping of local source code id to its database id.
     * @param specificDataHolder Specific data holder for requests.
     * @param revision Merged revision.
     * @param latestCommittedRevision Latest committed revision.
     * @param mapNodeDbIdResourceDbId Mapping of node database id to its resource database id.
     * @param nodesExistedBefore Set of node ids for nodes which existed prior to merging
     */
    public StandardProcessorContext(Transaction dbTransaction, Map<String, Long> mapNodeIdToDbId,
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
    public StandardProcessorContext() {
        super();
        dbTransaction = null;
    }

    /**
     * Initializes context with revisions.
     */
    public StandardProcessorContext(Double revision, Double latestCommittedRevision) {
        super();
        this.revision = revision;
        this.latestCommittedRevision = latestCommittedRevision;
        this.dbTransaction = null;
    }

    /**
     * Adds transaction object to context.
     * @param tx Database transaction object.
     */
    public void setDbTransaction(Transaction tx) {
        dbTransaction = tx;
    }

    /**
     * @return Database transaction object
     */
    public Transaction getDbTransaction() {
        return dbTransaction;
    }
}
