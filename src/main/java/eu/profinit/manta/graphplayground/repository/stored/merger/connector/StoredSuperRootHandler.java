package eu.profinit.manta.graphplayground.repository.stored.merger.connector;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;

/**
 * {@link MantaNodeLabel#SUPER_ROOT} node handler.
 * Used in a stored procedures.
 *
 * @author tfechtner
 * @author tpolacok
 */
public class StoredSuperRootHandler extends AbstractStoredRootHandler {

    @Override
    public DatabaseStructure.NodeProperty getNodeProperty() {
        return DatabaseStructure.NodeProperty.SUPER_ROOT;
    }

    @Override
    public MantaNodeLabel getMantaNodeLabel() {
        return MantaNodeLabel.SUPER_ROOT;
    }
}
