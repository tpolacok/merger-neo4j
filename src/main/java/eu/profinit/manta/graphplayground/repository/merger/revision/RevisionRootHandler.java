package eu.profinit.manta.graphplayground.repository.merger.revision;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.LicenseException;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.model.manta.revision.RevisionModel;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import eu.profinit.manta.graphplayground.repository.merger.connector.AbstractRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.connector.GraphCreation;
import eu.profinit.manta.graphplayground.repository.merger.connector.GraphOperation;
import eu.profinit.manta.graphplayground.repository.merger.permission.AllRepositoryPermissionProvider;

/**
 * Class for the management of revision root and revision tree in the database.
 *
 * @author pholecek
 * @author tfechtner
 * @author tpolacok
 */
@Repository
public class RevisionRootHandler extends AbstractRootHandler {

    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(RevisionRootHandler.class);

    /**
     * Error message for no revision existing.
     */
    public static final String NO_REVISION_IN_REP = "There is not any committed version in the repository yet.";

    /**
     * Double in Titan database has maximum scale up to 6 decimal digits
     * (double can be serialized only up to 6 decimal digits)
     */
    public static final Integer SCALE = 6;

    /**
     * Rounding mode used when cutting rest of the decimal digits.
     */
    public static final RoundingMode ROUNDING_MODE = RoundingMode.DOWN;

    /**
     * Technical revision number 0.000000. Technical revision is the first revision (=minimum revision number).
     */
    public static final double TECHNICAL_REVISION_NUMBER = new BigDecimal("0.000000").setScale(SCALE, ROUNDING_MODE)
            .doubleValue();

    /**
     * Graph operation instance.
     */
    private final GraphOperation graphOperation = new GraphOperation();

    /**
     * Graph creation instance.
     */
    private final GraphCreation graphCreation = new GraphCreation(graphOperation);

    //    /** Revision max value is the maximum possible double value stored in the Titan database (serialized).
    //     *  Can NOT use Double.MAX_VALUE -> out of range.
    //     *  Computation of the max serialized double value is taken from the Titan DoubleSerializer class.
    //     */
    //    public static long MULTIPLIER = 0;
    //    static {
    //        MULTIPLIER = 1;
    //        for (int i = 0; i < SCALE; i++) MULTIPLIER *= 10;
    //    }
    //    public static final double REVISION_MAX_NUMBER = Long.MAX_VALUE*1.0 / (MULTIPLIER+1);

    // FIXME cannot use higher value greater than Integer.MAX_VALUE due to some unknown reason in the failing test Dev1731Test (dataflow.routine)
    public static final double REVISION_MAX_NUMBER = Integer.MAX_VALUE;

    /**
     * Interval of all revisions.
     */
    public static final RevisionInterval EVERY_REVISION_INTERVAL = new RevisionInterval(TECHNICAL_REVISION_NUMBER,
            REVISION_MAX_NUMBER);

    /**
     * The increment value when creating a new minor revision.
     */
    public static final BigDecimal MINOR_REVISION_DELTA = new BigDecimal("0.000001").setScale(SCALE, ROUNDING_MODE);

    /**
     * The maximum minor revision number within one major revision.
     */
    public static final BigDecimal MINOR_REVISION_MAXIMUM_NUMBER = new BigDecimal("0.999999").setScale(SCALE,
            ROUNDING_MODE);

    /**
     * Decimal format with 6 decimal digits for printing.
     */
    private NumberFormat formatter = new DecimalFormat("#0.000000");

    /**
     * Lock for accessing revision vertices.
     */
    private final ReadWriteLock revisionsLock = new ReentrantReadWriteLock();

    /**
     * Lock for accessing {@link MantaNodeLabel#REVISION_ROOT} vertex.
     */
    private final Object rootLock = new Object();

    /**
     * {@code true} if versioning system is online.
     */
    private final boolean isVersionSystemOn = true;

    /**
     * {@code true} if additional merging is possible.
     */
    private final boolean isAdditionalMergeOn = false;

    @Override
    public DatabaseStructure.NodeProperty getNodeProperty() {
        return DatabaseStructure.NodeProperty.REVISION_ROOT;
    }

    @Override
    public MantaNodeLabel getMantaNodeLabel() {
        return MantaNodeLabel.REVISION_ROOT;
    }

    @Override
    public Node ensureRootExistance(Transaction transaction) {
        synchronized (rootLock) {
            return super.ensureRootExistance(transaction);
        }
    }

    @Override
    public Node getRoot(Transaction transaction) {
        synchronized (rootLock) {
            return super.getRoot(transaction);
        }
    }

    /**
     * Create new major revision. Latest revision has to be committed.
     * New major revision is always greater by 1 than the previous major revision.
     * New major revision has always minor revision .000000
     * <p>
     * Example 1: latestCommittedRevision = 1.000000 => newMajorRevision = 2.000000
     * Example 2: latestCommittedRevision = 1.123456 => newMajorRevision = 2.000000
     *
     * @param tx  transaction for the access to the database
     * @return newly created revision number
     */
    public Double createMajorRevision(Transaction tx) {
        revisionsLock.writeLock().lock();
        try {
            Node revisionRootNode = getRoot(tx);
            Double latestCommRevNumber = getLatestCommittedRevisionNumber(tx);
            Double latestUncommRevNumber = getLatestUncommittedRevisionNumber(tx);
            Double newRevNumber = getNextMajorRevNumber(latestCommRevNumber, latestUncommRevNumber);

            LOGGER.info("Creating major revision {}.", formatter.format(newRevNumber));
            createRevisionNode(tx, revisionRootNode, newRevNumber, latestCommRevNumber);
            //TODO scripts
//            revertUncommittedScripts();
            return newRevNumber;
        } finally {
            revisionsLock.writeLock().unlock();
        }
    }

    /**
     * Get next major revision number
     *
     * @param latestCommRevNumber  latest committed revision number
     * @param latestUncommRevNumber latest uncommitted revision number
     * @return  next major revision number or technical revision number if no revision exists
     */
    private Double getNextMajorRevNumber(Double latestCommRevNumber, Double latestUncommRevNumber) {
        if (latestCommRevNumber == null && latestUncommRevNumber == null) {
            return TECHNICAL_REVISION_NUMBER;
        } else if (latestCommRevNumber == null
                || (latestUncommRevNumber != null && latestUncommRevNumber > latestCommRevNumber)) {
            throw new RevisionException("Cannot create a new revision without committing the previous one.");
        } else {
            return Math.floor(latestCommRevNumber + 1);
        }
    }
//
//    /**
//     * Get next minor revision number
//     *
//     * @param latestCommRevNumber  latest committed revision number
//     * @param latestUncommRevNumber  latest uncommitted revision number
//     * @return next minor revision number or technical revision number if no revision exists
//     */
//    private Double getNextMinorRevNumber(Double latestCommRevNumber, Double latestUncommRevNumber) {
//        if (latestCommRevNumber == null && latestUncommRevNumber == null) {
//            return TECHNICAL_REVISION_NUMBER;
//        } else if (latestCommRevNumber == null
//                || (latestUncommRevNumber != null && latestUncommRevNumber > latestCommRevNumber)) {
//            throw new RevisionException("Cannot create a new revision without committing the previous one.");
//        } else {
//            BigDecimal integerPart = BigDecimal.valueOf(Math.floor(latestCommRevNumber)).setScale(SCALE, ROUNDING_MODE);
//            BigDecimal maximumMinorRevision = integerPart.add(MINOR_REVISION_MAXIMUM_NUMBER);
//            BigDecimal latestCommittedRevisionBigDecimal = BigDecimal.valueOf(latestCommRevNumber).setScale(SCALE,
//                    ROUNDING_MODE);
//            if (maximumMinorRevision.equals(latestCommittedRevisionBigDecimal)) {
//                throw new IllegalStateException(
//                        "Maximum minor revision reached (latestCommittedRevision=" + latestCommRevNumber + ").");
//            } else {
//                return latestCommittedRevisionBigDecimal.add(MINOR_REVISION_DELTA).doubleValue();
//            }
//        }
//    }
//
    /**
     * Create new revision node
     *
     * @param revisionRootNode  revision root node
     * @param newRevNumber  revision number of the revision node to-be-created
     * @param latestCommRevNumber  latest committed revision number
     * @param tx  transaction for the access to the database
     */
    private void createRevisionNode(Transaction tx, Node revisionRootNode, Double newRevNumber, Double latestCommRevNumber) {
        // get the previous revision node
        Node prevRevNode;
        if (newRevNumber == TECHNICAL_REVISION_NUMBER) {
            // technical revision is always the first revision -> has no preceding revision
            prevRevNode = null;
        } else {
            // previous revision is always the latest committed revision
            prevRevNode = getSpecificVertex(tx, latestCommRevNumber, revisionRootNode).orElse(null);
        }

        // newly created revision node is the latest revision -> has no following revision
        Node nextRevNode = null;

        graphCreation.createRevisionNode(tx, revisionRootNode, newRevNumber, prevRevNode, nextRevNode,
                false, new Date());
    }
//
//    /**
//     * Revert uncommitted scripts
//     */
//    private void revertUncommittedScripts() {
//        // Pro jistotu revertujeme informace o skriptech, ktere jeste nebyly commitnuty
//        int revertedScriptCount = scriptMetadataService.revertUncommittedScripts();
//        LOGGER.info(MessageFormat.format("{0} script hash(es) reverted.", revertedScriptCount));
//    }
//
    /**
     * Commit a specific revision. Revision can be committed only if it is the latest uncommitted revision.
     * When the latest uncommitted revision is committed, the latest uncommitted revision is set to (-1),
     * representing that no revision is uncommitted.
     *
     * @param transaction  Transaction for the access to the database.
     * @param revision Number of the revision to be committed.
     */
    public void commitRevision(Transaction transaction, Double revision) {
        revisionsLock.writeLock().lock();
        try {
            doCommitRevision(transaction, revision);
        } finally {
            revisionsLock.writeLock().unlock();
        }
    }

    /**
     * Commit the latest revision (revision with the highest number).
     * Latest revision can be either committed or uncommitted. Committed revision cannot be committed again.
     *
     * @param transaction  Transaction for the access to the database.
     */
    public void commitLatestRevision(Transaction transaction) {
        revisionsLock.writeLock().lock();
        try {
            Node revisionRootNode = getRoot(transaction);
            Value revisionNumber = revisionRootNode.get(DatabaseStructure.NodeProperty.LATEST_UNCOMMITTED_REVISION.t());
            if (revisionNumber == null || revisionNumber.asDouble() == -1) {
                throw new RevisionException("Latest revision is already committed.");
            }

            doCommitRevision(transaction, revisionNumber.asDouble());
        } finally {
            revisionsLock.writeLock().unlock();
        }
    }

    /**
     * Commit a specific revision. Revision can be committed only if it is the latest uncommitted revision.
     * When the latest uncommitted revision is committed, the latest uncommitted revision is set to (-1),
     * representing that no revision is uncommitted.
     *
     * @param transaction Transaction for the access to the database.
     * @param revision Number of the revision to be committed.
     *
     * @implNote No locking is done in this method. The client must handle repository locks.
     */
    private void doCommitRevision(Transaction transaction, Double revision) {

        if (revision == null) {
            throw new RevisionException("Revision cannot be null.");
        }
        Node revisionRootNode = getRoot(transaction);
        // Najdu uzel se zadaným cislem revize
        Node revisionVertex = getSpecificVertex(transaction, revision, revisionRootNode)
                .orElseThrow(() -> new RevisionException("Revision " + formatter.format(revision) + " does not exist."));
        RevisionModel revisionModel = new RevisionModel(revisionVertex);

        if (revisionModel.isCommitted()) {
            throw new RevisionException("Trying to commit an already committed revision.");
        }

        // Check revision count against the license.
        validateRevisionsCount(transaction);

        // Zkusime commitnout informace o commitnutych skriptech
        //TODO script counting
//        Collection<String> sourceCodeHashes = sourceCodeCounter.getSourceCodeHashes(transaction);
//        int storageResult = scriptMetadataService.commitScripts(licenseHolder.getLicense().getMaxScriptCount(ScriptMetadataConfig.SCRIPT_COUNT_EXCEED_TOLERANCE),
//                ScriptMetadataConfig.LAST_SCRIPT_COUNT_PERIOD, new CollectionsMerger<String>(sourceCodeHashes));
//        if (storageResult != 0) {
//            // Pokud se to nepovede, vyhodime vyjimku
//            int limit = licenseHolder.getLicense().getMaxScriptCount();
//            throw new LicenseException(MessageFormat.format(
//                    "Scripts cannot be processed because script count ({0}) would exceed the limit {1}.",
//                    storageResult, limit));
//        }

        // Není commitnutý - nastavím příznak na true a čas commitu
        Map<String, Object> revisionProps = new HashMap<>();
        revisionProps.put(DatabaseStructure.NodeProperty.REVISION_NODE_COMMIT_TIME.t(), new Date().toString());
        revisionProps.put(DatabaseStructure.NodeProperty.REVISION_NODE_COMMITTED.t(), true);
        graphCreation.addNodeProperty(transaction, revisionVertex.id(), revisionProps);

        Map<String, Object> revisionRootProps = new HashMap<>();
        revisionRootProps.put(DatabaseStructure.NodeProperty.LATEST_COMMITTED_REVISION.t(), revisionModel.getRevision());
        revisionRootProps.put(DatabaseStructure.NodeProperty.LATEST_UNCOMMITTED_REVISION.t(), -1.0);
        graphCreation.addNodeProperty(transaction, revisionRootNode.id(), revisionRootProps);
        LOGGER.info("Revision {} committed.", formatter.format(revision));
    }

    /**
     * Check if the number of revisions in the repository is valid.
     *
     * @param transaction Transaction for the access to the database.
     *
     * @throws LicenseException if the validation fails.
     *
     * @implSpec
     *
     * There validates committed revisions in repository (except the technical revision)
     * against a number of revisions allowed by the license.
     *
     * Theoretically, even uncommitted revision should be counted, but it is not, because we want to validate against a
     * hard limit.
     *
     * <i>Hard limit</i> - number of committed revisions + number of uncommitted revisions <= allowed revisions + 1
     */
    public void validateRevisionsCount(Transaction transaction) {
        //TODO license validation
//        int existingRevisions = getRevisionModels(transaction, false).size();
//        boolean isValid = licenseHolder.getLicense().isValidRevisionsCount(existingRevisions);
//        if (!isValid) {
//            throw new RevisionException ("The limit of committed revisions allowed by the license has been exceeded. " +
//                    "At the moment, there is " + existingRevisions + " committed revisions in the repository.");
//        }
    }
//
//    /**
//     * Provede callback s držením R zámku pro revize {@link #revisionsLock}.
//     * @param operation operace k vykonání
//     * @return výsledek operace
//     *
//     * @param <T> návrratová hodnota callbacku
//     */
//    public <T> T executeRevisionReadOperation(RevisionLockedOperation<T> operation) {
//        revisionsLock.readLock().lock();
//        try {
//            return operation.call();
//        } finally {
//            revisionsLock.readLock().unlock();
//        }
//    }
//
//    /**
//     * Provede callback s držením R/W zámku pro revize {@link #revisionsLock}.
//     * @param operation operace k vykonání
//     * @return výsledek operace
//     *
//     * @param <T> návrratová hodnota callbacku
//     */
//    public <T> T executeRevisionWriteOperation(RevisionLockedOperation<T> operation) {
//        revisionsLock.writeLock().lock();
//        try {
//            return operation.call();
//        } finally {
//            revisionsLock.writeLock().unlock();
//        }
//    }
//
    /**
     * Check, whether it is possible to perform merge operation in the specified revision.
     * Merge can be performed only in the latest uncommitted revision. Cannot merge into an
     * already committed revision.
     *
     * @param transaction  transaction for the access to the database
     * @param revision  number of the checked revision
     * @throws RevisionException  revision does not meet the conditions
     */
    public void checkMergeConditions(Transaction transaction, Double revision) {
        if (revision == null) {
            throw new RevisionException("Revision must not be null.");
        }

        Double latestUncommittedRevision = getLatestUncommittedRevisionNumber(transaction);

        if (latestUncommittedRevision == null) {
            throw new RevisionException(
                    "No uncommitted revision exists. Can merge only to the latest uncommitted revision.");
        }
        if (!latestUncommittedRevision.equals(revision)) {
            throw new RevisionException("Cannot merge to the revision " + revision + ". "
                    + "Can merge only to the latest uncommitted revision " + latestUncommittedRevision);
        }
    }

    /**
     * Check, whether it is possible to perform delete operation in the specified revision.
     * Delete can be performed only in the latest uncommitted revision. Cannot delete in an
     * already committed revision.
     *
     * @param transaction  transaction for the access to the database
     * @param revision  number of the checked revision
     * @throws RevisionException  revision does not meet the conditions
     */
    public void checkDeleteConditions(Transaction transaction, Double revision) {
        if (revision == null) {
            throw new RevisionException("Revision must not be null.");
        }

        Double latestUncommittedRevision = getLatestUncommittedRevisionNumber(transaction);

        if (latestUncommittedRevision == null) {
            throw new RevisionException(
                    "No uncommitted revision exists. Can delete only in the latest uncommitted revision.");
        }
        if (!latestUncommittedRevision.equals(revision)) {
            throw new RevisionException("Cannot delete in the revision " + revision + ". "
                    + "Can delete only in the latest uncommitted revision " + latestUncommittedRevision);
        }
    }
//
//    /**
//     * Removes revision node. The revision can be both committed or uncommitted.
//     *
//     * @param databaseHolder  database holder
//     * @param revisionNumber  number of the revision to be destroyed
//     * @param isCommitted  whether the revision has already been committed
//     */
//    public void removeRevisionNode(DatabaseHolder databaseHolder, final Double revisionNumber, boolean isCommitted) {
//        revisionsLock.writeLock().lock();
//        try {
//            databaseHolder.runInTransaction(TransactionLevel.WRITE_EXCLUSIVE, new TransactionCallback<Object>() {
//                @Override
//                public Object callMe(TitanTransaction transaction) {
//                    removeRevisionNode(transaction, revisionNumber);
//                    if (!isCommitted) {
//                        revertUncommittedScripts();
//                    }
//                    return null;
//                }
//
//                @Override
//                public String getModuleName() {
//                    return MODULE_NAME;
//                }
//
//            });
//        } finally {
//            revisionsLock.writeLock().unlock();
//        }
//    }
//
//    /**
//     * Removes revision node. The revision can be both committed or uncommitted.
//     *
//     * @param transaction  transaction for the access to the database
//     * @param revisionNumber  number of the revision to be destroyed
//     */
//    public void removeRevisionNode(TitanTransaction transaction, Double revisionNumber) {
//        if (revisionNumber == null) {
//            throw new RevisionException("The revision number must not be null.");
//        }
//        revisionsLock.writeLock().lock();
//        try {
//            Vertex revisionRootNode = getRoot(transaction);
//            getSpecificVertex(revisionNumber, revisionRootNode)
//                    .ifPresent(revisionVertex -> removeRevisionNode(revisionNumber, revisionVertex, revisionRootNode));
//        } finally {
//            revisionsLock.writeLock().unlock();
//        }
//    }
//
//    /**
//     * Removes revision node. The revision can be both committed or uncommitted. It destroys the node and removes any
//     * information about it from the root node.
//     *
//     * The method might throw and exception if there there is a newer revision than the one specified.
//     *
//     * @param revisionNumber number of the revision to be destroyed
//     * @param revisionVertex revision vertex to be removed
//     * @param revisionRootNode revision root node - contains all revisions as it's direct children
//     */
//    private void removeRevisionNode(Double revisionNumber, Vertex revisionVertex, Vertex revisionRootNode) {
//        boolean isCommitted = revisionVertex.getProperty(NodeProperty.REVISION_NODE_COMMITTED.t());
//
//        Double previousRevisionNumber = revisionVertex.getProperty(NodeProperty.REVISION_NODE_PREVIOUS_REVISION.t());
//        Double nextRevisionNumber = revisionVertex.getProperty(NodeProperty.REVISION_NODE_NEXT_REVISION.t());
//
//        removeLastRevisionFromGraph(revisionNumber, isCommitted, previousRevisionNumber, nextRevisionNumber,
//                revisionRootNode);
//
//        getSpecificVertex(previousRevisionNumber, revisionRootNode)
//                // set next revision of the previous revision to the next revision of the current revision
//                .ifPresent(previousRevisionVertex -> previousRevisionVertex.setProperty(
//                        NodeProperty.REVISION_NODE_NEXT_REVISION.t(), nextRevisionNumber));
//        // finally, remove the revision node
//        revisionVertex.remove();
//    }
//
//    /**
//     * Removes record about the last revision from the graph root node. If the revision has already been committed,
//     * there should be no uncommitted revision (otherwise throws an exception).
//     * @param revisionNumber number of the last revision
//     * @param isCommitted whether the revision has already been committed
//     * @param previousRevisionNumber ID of previous revision relative the removed one (-1 means no revision)
//     * @param nextRevisionNumber ID of next revision relative the removed one (-1 means no revision)
//     * @param revisionRootNode revision root from which record about the last revision will be removed
//     */
//    private void removeLastRevisionFromGraph(double revisionNumber, boolean isCommitted, double previousRevisionNumber,
//                                             double nextRevisionNumber, Vertex revisionRootNode) {
//        double latestCommittedRev = revisionRootNode.getProperty(NodeProperty.LATEST_COMMITTED_REVISION.t());
//        double latestUncommittedRev = revisionRootNode.getProperty(NodeProperty.LATEST_UNCOMMITTED_REVISION.t());
//        if (isCommitted) {
//            if (latestCommittedRev != revisionNumber) {
//                String message = MessageFormat.format("Revision number {} is not the latest committed "
//                        + "revision. Latest committed revision is {}", revisionNumber, latestCommittedRev);
//                throw new RevisionException(message);
//            }
//            if (nextRevisionNumber > RevisionRootHandler.TECHNICAL_REVISION_NUMBER) {
//                String message = MessageFormat.format("Latest revision {} has an successor {}.",
//                        revisionNumber, latestCommittedRev);
//                throw new IllegalStateException(message);
//            }
//            if (latestUncommittedRev >= 0) {
//                String message = MessageFormat.format("Trying to remove revision {} while there is an "
//                        + "uncommitted revision {}", revisionNumber, latestCommittedRev);
//                throw new RevisionException(message);
//            }
//            revisionRootNode.setProperty(NodeProperty.LATEST_COMMITTED_REVISION.t(), previousRevisionNumber);
//        } else {
//            if (latestUncommittedRev < 0) {
//                throw new IllegalStateException("Latest uncommitted revision not found despite there is an "
//                        + "uncommitted revision " + revisionNumber);
//            }
//            // reset the latest uncommitted revision number in the revision root, latest committed revision
//            // remains the same
//            revisionRootNode.setProperty(NodeProperty.LATEST_UNCOMMITTED_REVISION.t(), -1.0);
//        }
//    }
//
//    /**
//     * Remove revision interval.
//     *
//     * @param databaseHolder  database holder
//     * @param revisionInterval  interval of revisions that are to be removed
//     */
//    public void removeRevisionInterval(DatabaseHolder databaseHolder, final RevisionInterval revisionInterval) {
//        revisionsLock.writeLock().lock();
//        try {
//            databaseHolder.runInTransaction(TransactionLevel.WRITE_EXCLUSIVE, new TransactionCallback<Object>() {
//                @Override
//                public Object callMe(TitanTransaction transaction) {
//                    removeRevisionInterval(transaction, revisionInterval);
//                    return null;
//                }
//
//                @Override
//                public String getModuleName() {
//                    return MODULE_NAME;
//                }
//
//            });
//        } finally {
//            revisionsLock.writeLock().unlock();
//        }
//    }
//
//    /**
//     * Remove revision interval.
//     *
//     * @param transaction  transaction for the access to the database
//     * @param revisionInterval  interval of revisions that are to be removed
//     */
//    public void removeRevisionInterval(TitanTransaction transaction, RevisionInterval revisionInterval) {
//        revisionsLock.writeLock().lock();
//        try {
//            Vertex revisionRootNode = getRoot(transaction);
//            List<Vertex> revisions = GraphOperation.getAdjacentVertices(revisionRootNode, Direction.OUT,
//                    revisionInterval, AllRepositoryPermissionProvider.getInstance(), EdgeLabel.HAS_REVISION);
//            for (Vertex rev : revisions) {
//                rev.remove();
//            }
//        } finally {
//            revisionsLock.writeLock().unlock();
//        }
//    }
//
//    /**
//     * Get the latest committed revision number
//     *
//     * @param databaseHolder  database holder
//     * @return latest committed revision number or null, if no revision was committed yet
//     */
//    public Double getLatestCommittedRevisionNumber(DatabaseHolder databaseHolder) {
//        return databaseHolder.runInTransaction(TransactionLevel.READ, new TransactionCallback<Double>() {
//            @Override
//            public String getModuleName() {
//                return MODULE_NAME;
//            }
//
//            @Override
//            public Double callMe(TitanTransaction transaction) {
//                return getLatestCommittedRevisionNumber(transaction);
//            }
//        });
//    }

    /**
     * Get the latest committed revision number
     *
     * @param transaction  transaction for the access to the database
     * @return latest committed revision number or null, if no revision was committed yet
     */
    public Double getLatestCommittedRevisionNumber(Transaction transaction) {
        Node revisionRootNode = getRoot(transaction);
        Value latestCommittedRevision = revisionRootNode.get(DatabaseStructure.NodeProperty.LATEST_COMMITTED_REVISION.t());
        if (latestCommittedRevision.isNull() || latestCommittedRevision.asDouble() == -1.0) {
            return null;
        } else {
            return latestCommittedRevision.asDouble();
        }
    }

    /**
     * Get the latest uncommitted revision number
     *
     * @param transaction  transaction for the access to the database
     * @return latest uncommitted revision number or null, if no uncommitted revision exists
     */
    public Double getLatestUncommittedRevisionNumber(Transaction transaction) {
        Node revisionRootNode = getRoot(transaction);
        Value latestUncommittedRevision = revisionRootNode.get(DatabaseStructure.NodeProperty.LATEST_UNCOMMITTED_REVISION.t());
        if (latestUncommittedRevision.isNull() || latestUncommittedRevision.asDouble() == -1.0) {
            return null;
        } else {
            return latestUncommittedRevision.asDouble();
        }
    }
//
//    /**
//     * Get the latest committed revision number.
//     *
//     * @param databaseHolder  database holder
//     * @return latest committed revision number or null, if no revision was committed yet
//     */
//    public Double getHeadNumber(DatabaseHolder databaseHolder) {
//        return getLatestCommittedRevisionNumber(databaseHolder);
//    }
//
    /**
     * Get the latest committed revision number.
     *
     * @param transaction  transaction to access the database
     * @return latest committed revision number or null, if no revision was committed yet
     */
    public Double getHeadNumber(Transaction transaction) {
        return getLatestCommittedRevisionNumber(transaction);
    }

    /**
     * Get the latest revision number, regardless of the commit status.
     *
     * @param transaction  transaction for the access to the database
     * @return latest revision number or null, if no revision was created yet
     */
    public Double getLatestRevisionNumber(Transaction transaction) {
        Node revisionRootNode = getRoot(transaction);
        Value latestCommittedRevision = revisionRootNode.get(DatabaseStructure.NodeProperty.LATEST_COMMITTED_REVISION.t());
        Value latestUncommittedRevision = revisionRootNode.get(DatabaseStructure.NodeProperty.LATEST_UNCOMMITTED_REVISION.t());

        if (latestCommittedRevision == null && latestUncommittedRevision == null) {
            return null;
        } else if (latestCommittedRevision == null || latestUncommittedRevision.asDouble() > latestCommittedRevision.asDouble()) {
            return latestUncommittedRevision.asDouble();
        } else {
            return latestCommittedRevision.asDouble();
        }
    }

//    /**
//     * Get the latest revision number, regardless of the commit status.
//     *
//     * @param databaseHolder  database holder
//     * @return latest revision number or null, if no revision was created yet
//     */
//    public Double getLatestRevisionNumber(DatabaseHolder databaseHolder) {
//        return databaseHolder.runInTransaction(TransactionLevel.READ, new TransactionCallback<Double>() {
//            @Override
//            public String getModuleName() {
//                return MODULE_NAME;
//            }
//
//            @Override
//            public Double callMe(TitanTransaction transaction) {
//                return getLatestRevisionNumber(transaction);
//            }
//        });
//    }
//
//    /**
//     * Vrátí nejstarší commitnutou revizi.
//     * @param databaseHolder držák na databázi
//     * @return nejstarší revize nebo null, když není žádní commitnutá
//     */
//    public RevisionModel getOldestModel(DatabaseHolder databaseHolder) {
//        return databaseHolder.runInTransaction(TransactionLevel.READ, new TransactionCallback<RevisionModel>() {
//
//            @Override
//            public RevisionModel callMe(TitanTransaction transaction) {
//                return getOldestModel(transaction);
//            }
//
//            @Override
//            public String getModuleName() {
//                return MODULE_NAME;
//            }
//        });
//    }
//
//    /**
//     * Vrátí nejstarší commitnutou revizi.
//     * @param transaction transakce pro přístup k db
//     * @return nejstarší revize nebo null, když není žádní commitnutá
//     */
//    public RevisionModel getOldestModel(TitanTransaction transaction) {
//        Vertex revisionRootNode = getRoot(transaction);
//        List<Vertex> revisionVertices = GraphOperation.getAdjacentVertices(revisionRootNode, Direction.OUT,
//                EVERY_REVISION_INTERVAL, AllRepositoryPermissionProvider.getInstance(), EdgeLabel.HAS_REVISION);
//
//        Double lowestRevisionNumber = REVISION_MAX_NUMBER;
//        Vertex oldestRevisionVertex = null;
//        for (Vertex currentVertex : revisionVertices) {
//            Double currentVertexRevision = currentVertex.getProperty(NodeProperty.REVISION_NODE_REVISION.t());
//            if (currentVertexRevision == null) {
//                throw new RevisionException("Revision node property revision not set.");
//            }
//
//            Boolean isCommitted = currentVertex.getProperty(NodeProperty.REVISION_NODE_COMMITTED.t());
//            if (BooleanUtils.isTrue(isCommitted) && currentVertexRevision < lowestRevisionNumber) {
//                lowestRevisionNumber = currentVertexRevision;
//                oldestRevisionVertex = currentVertex;
//            }
//        }
//
//        if (oldestRevisionVertex != null) {
//            return new RevisionModel(oldestRevisionVertex);
//        } else {
//            return null;
//        }
//    }
//
//    /**
//     * Získá model pro specifickou revizi.
//     * @param transaction transakce pro přístup k db
//     * @param revisionNumber číslo revize, který se má najít
//     * @return model pro specifickou revizi, nebo null pokud neexistuje
//     */
//    public RevisionModel getSpecificModel(TitanTransaction transaction, Double revisionNumber) {
//        if (revisionNumber == null) {
//            throw new RevisionException("The revision number must not be null.");
//        }
//
//        Vertex revisionRootNode = getRoot(transaction);
//        return getSpecificVertex(revisionNumber, revisionRootNode)
//                .map(RevisionModel::new)
//                .orElse(null);
//    }
//
//    /**
//     * Získá model pro specifickou revizi.
//     * @param databaseHolder držák na databáze
//     * @param revisionNumber číslo revize, který se má najít
//     * @return model pro specifickou revizi, nebo null pokud neexistuje
//     */
//    public RevisionModel getSpecificModel(DatabaseHolder databaseHolder, final Double revisionNumber) {
//        return databaseHolder.runInTransaction(TransactionLevel.READ, new TransactionCallback<RevisionModel>() {
//
//            @Override
//            public String getModuleName() {
//                return MODULE_NAME;
//            }
//
//            @Override
//            public RevisionModel callMe(TitanTransaction transaction) {
//                return getSpecificModel(transaction, revisionNumber);
//            }
//        });
//    }
//
    /**
     * Attempts to find vertex in specific revision.
     *
     * @param revisionNumber Searched revision.
     * @param revisionRootNode Revision root vertex.
     * @return Optional containing found vertex or empty.
     */
    private Optional<Node> getSpecificVertex(Transaction tx, Double revisionNumber, Node revisionRootNode) {
        RevisionInterval revInterval = new RevisionInterval(revisionNumber, revisionNumber);
        List<Node> revisionVertices = graphOperation.getRevisionNode(tx, revisionRootNode,
                AllRepositoryPermissionProvider.getInstance(), revInterval);
        if (revisionVertices.size() == 1) {
            return Optional.of(revisionVertices.get(0));
        } else if (revisionVertices.isEmpty()) {
            return Optional.empty();
        } else {
            throw new RevisionException("There are more (" + revisionVertices.size()
                    + ") revision vertices for one revision (" + revisionNumber + ").");
        }
    }
//
//    public Double getPreviousRevisionNumber(DatabaseHolder databaseHolder, final Double revisionNumber) {
//        return databaseHolder.runInTransaction(TransactionLevel.READ, new TransactionCallback<Double>() {
//
//            @Override
//            public String getModuleName() {
//                return MODULE_NAME;
//            }
//
//            @Override
//            public Double callMe(TitanTransaction transaction) {
//                return getPreviousRevisionNumber(transaction, revisionNumber);
//            }
//        });
//    }
//
//    /**
//     * Get previous revision number (preceding revision with the highest revision number) of another revision number.
//     * Previous revision number is saved as a property in every revision node.
//     * <p>
//     * Example:
//     * Revisions: 0.000000, 1.000000, 1.000001, 1.000002, 2.000000, 2.000001
//     * previousRevisionNumber(2.000001) -> 2.000000
//     * previousRevisionNumber(2.000000) -> 1.000002
//     * previousRevisionNumber(0.000000) -> null
//     *
//     * @param transaction  transaction for the access to the database
//     * @param revisionNumber  number of the revision for which the previous revision number is searched
//     * @return previous revision number or null, if no previous revision exists
//     */
//    public Double getPreviousRevisionNumber(TitanTransaction transaction, Double revisionNumber) {
//        Vertex revisionRootNode = getRoot(transaction);
//        return getSpecificVertex(revisionNumber, revisionRootNode)
//                .map(revNode -> (Double) revNode.getProperty(NodeProperty.REVISION_NODE_PREVIOUS_REVISION.t()))
//                .filter(previousRevisionNumber -> previousRevisionNumber != -1.0)
//                .orElse(null);
//    }
//
//    /**
//     * Ziska seznam vsech commitnutych revizi serazenych vzestupně dle cisla revize.
//     * @param databaseHolder Drzak databaze
//     * @return seznam commitnutych revizi, nikdy {@code null}
//     */
//    //use getRevisionModels instead
//    @Deprecated
//    public List<RevisionModel> getCommitedRevisionModels(DatabaseHolder databaseHolder) {
//        return getRevisionModels(databaseHolder,false);
//    }
//
//    /**
//     * Returns list of all revisions ascending by revision.
//     *
//     * @param databaseHolder Database holder
//     * @param  includeUncommitted true if uncommitted version should be included
//     * @return List of all revisions, never {@code null}
//     */
//    public List<RevisionModel> getRevisionModels(DatabaseHolder databaseHolder, boolean includeUncommitted) {
//        List<RevisionModel> revisionModelList = databaseHolder.runInTransaction(TransactionLevel.READ,
//                new TransactionCallback<List<RevisionModel>>() {
//                    @Override
//                    public String getModuleName() {
//                        return MODULE_NAME;
//                    }
//
//                    @Override
//                    public List<RevisionModel> callMe(TitanTransaction transaction) {
//                        return getRevisionModels(transaction, includeUncommitted);
//                    }
//                });
//        return revisionModelList;
//    }
//
//    /**
//     * Returns list of all revisions ascending by revision.
//     *
//     * @param transaction Transaction for the access to the database.
//     * @param  includeUncommitted true if uncommitted version should be included
//     *
//     * @return List of all revisions, never {@code null}
//     */
//    public List<RevisionModel> getRevisionModels(TitanTransaction transaction, boolean includeUncommitted) {
//        List<RevisionModel> revisionModelList = new ArrayList<>();
//
//        Vertex revisionRoot = getRoot(transaction);
//        List<Vertex> revisionVertices = GraphOperation.getAdjacentVertices(revisionRoot, Direction.OUT,
//                RevisionRootHandler.EVERY_REVISION_INTERVAL,
//                AllRepositoryPermissionProvider.getInstance(), EdgeLabel.HAS_REVISION);
//        for (Vertex vertex : revisionVertices) {
//            RevisionModel model = new RevisionModel(vertex);
//            if (model.isCommitted() || includeUncommitted) {
//                revisionModelList.add(model);
//            }
//        }
//
//        Collections.sort(revisionModelList);
//
//        return revisionModelList;
//    }
//
//
//
//    /**
//     * @return True, jestliže se má použít v systému verzování.
//     */
//    public boolean isVersionSystemOn() {
//        return isVersionSystemOn;
//    }
//
//    /**
//     * @param shouldVersionSystemOn True, jestliže se má použít v systému verzování.
//     */
//    public void setVersionSystemOn(boolean shouldVersionSystemOn) {
//        this.isVersionSystemOn = shouldVersionSystemOn;
//    }
//
//    /**
//     * @return true, jestliže se smít používat additional merge
//     */
//    public boolean isAdditionalMergeOn() {
//        return isAdditionalMergeOn;
//    }
//
//    /**
//     * @param shouldAdditionalMergeOn true, jestliže se smít používat additional merge
//     */
//    public void setAdditionalMergeOn(boolean shouldAdditionalMergeOn) {
//        this.isAdditionalMergeOn = shouldAdditionalMergeOn;
//    }
//
//    /**
//     * @param scriptMetadataService Sluzba pro praci s metadaty skriptu.
//     */
//    public void setScriptMetadataService(ScriptMetadataService scriptMetadataService) {
//        this.scriptMetadataService = scriptMetadataService;
//    }
//
//    /**
//     * @param sourceCodeCounter Pocitadlo zdrojovych kodu v databazi.
//     */
//    public void setSourceCodeCounter(SourceCodeCounter sourceCodeCounter) {
//        this.sourceCodeCounter = sourceCodeCounter;
//    }
//
//    /**
//     * @param licenseHolder Drzak licence.
//     */
//    public void setLicenseHolder(LicenseHolder licenseHolder) {
//        this.licenseHolder = licenseHolder;
//    }
}
