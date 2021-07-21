package eu.profinit.manta.graphplayground.repository.merger.connector;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import org.springframework.stereotype.Repository;

/**
 * Handler for {@link MantaNodeLabel#SUPER_ROOT} node.
 *
 * @author tfechtner
 */
@Repository
public class SuperRootHandler extends AbstractRootHandler {

    @Override
    public DatabaseStructure.NodeProperty getNodeProperty() {
        return DatabaseStructure.NodeProperty.SUPER_ROOT;
    }

    @Override
    public MantaNodeLabel getMantaNodeLabel() {
        return MantaNodeLabel.SUPER_ROOT;
    }
}
