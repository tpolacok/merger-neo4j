package eu.profinit.manta.graphplayground.repository.stored.merger.revision;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.AbstractStoredRootHandler;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.springframework.stereotype.Repository;

/**
 * Class for the management of {@link MantaNodeLabel#REVISION_ROOT} and revision tree in the database.
 * Used in a stored procedures.
 *
 * @author pholecek
 * @author tfechtner
 * @author tpolacok
 */
@Repository
public class StoredRevisionRootHandler extends AbstractStoredRootHandler {

    /**
     * Number of decimal digits for minor revisions.
     */
    public static final Integer SCALE = 6;

    /**
     * Lock for {@link MantaNodeLabel#REVISION_ROOT} access.
     */
    private final Object rootLock = new Object();

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
     * Get the latest committed revision number
     *
     * @param transaction  transaction for the access to the database
     * @return latest committed revision number or null, if no revision was committed yet
     */
    public Double getLatestCommittedRevisionNumber(Transaction transaction) {
        Node revisionRootNode = getRoot(transaction);
        Object latestCommittedRevision = revisionRootNode.getProperty(DatabaseStructure.NodeProperty.LATEST_COMMITTED_REVISION.t());
        if (latestCommittedRevision == null || (Double)latestCommittedRevision == -1.0) {
            return null;
        } else {
            return (Double)latestCommittedRevision;
        }
    }
}
