package eu.profinit.manta.graphplayground.repository.merger.parallel.processor;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;

import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphCreation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphOperation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSourceRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSuperRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionUtils;

/**
 * Modified merger processor to not create relationships between node and resource if the node has different parent.
 *
 * @author tpolacok
 */
public class ParallelStoredMergerProcessor extends StoredMergerProcessor {
    public ParallelStoredMergerProcessor(StoredSuperRootHandler superRootHandler, StoredSourceRootHandler sourceRootHandler, StoredRevisionRootHandler revisionRootHandler, StoredGraphOperation graphOperation, StoredGraphCreation graphCreation, StoredRevisionUtils revisionUtils, Log logger) {
        super(superRootHandler, sourceRootHandler, revisionRootHandler, graphOperation, graphCreation, revisionUtils, logger);
    }

    @Override
    protected Node createNodeResource(StoredProcessorContext context, Long parentVertexId, Long resourceVertexId,
                                      String name, String nodeType, RevisionInterval revisionInterval) {
        return graphCreation.createNode(context.getServerDbTransaction(), parentVertexId,
                DatabaseStructure.MantaRelationshipType.HAS_PARENT.t(), name,
                nodeType, revisionInterval);
    }

}
