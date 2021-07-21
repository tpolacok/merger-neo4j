package eu.profinit.manta.graphplayground.repository.stored.merger.connector;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link MantaNodeLabel#SOURCE_ROOT} node handler.
 * Used in a stored procedures.
 *
 * @author tfechtner
 * @author tpolacok
 */
public class StoredSourceRootHandler extends AbstractStoredRootHandler {
    /**
     * Logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(StoredSourceRootHandler.class);

    /**
     * Graph operation instance.
     */
    private StoredGraphOperation graphOperation;

    public StoredSourceRootHandler(StoredGraphOperation graphOperation) {
        this.graphOperation = graphOperation;
    }

    @Override
    public DatabaseStructure.NodeProperty getNodeProperty() {
        return DatabaseStructure.NodeProperty.SOURCE_ROOT;
    }

    @Override
    public MantaNodeLabel getMantaNodeLabel() {
        return MantaNodeLabel.SOURCE_ROOT;
    }

    /**
     * Retrieves identifier of the source code of given node.
     * @param vertex Node representing the source code.
     * @return Source code identifier.
     * @throws IllegalStateException Node missing required property.
     */
    public String getSourceNodeId(Node vertex) {
        Object property = vertex.getProperty(DatabaseStructure.NodeProperty.SOURCE_NODE_ID.t());
        if (property != null) {
            return property.toString();
        } else {
            throw new IllegalStateException("The vertex " + graphOperation.getName(vertex) + " does not have "
                    + DatabaseStructure.NodeProperty.SOURCE_NODE_ID.t() + " property.");
        }
    }
}
