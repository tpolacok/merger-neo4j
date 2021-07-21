package eu.profinit.manta.graphplayground.repository.merger.client;

import eu.profinit.manta.graphplayground.model.manta.merger.StandardProcessorContext;
import eu.profinit.manta.graphplayground.repository.merger.common.CommonMergerProcessorTest;
import eu.profinit.manta.graphplayground.repository.merger.connector.GraphCreation;
import eu.profinit.manta.graphplayground.repository.merger.connector.GraphOperation;
import eu.profinit.manta.graphplayground.repository.merger.connector.SourceRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.connector.SuperRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.processor.StandardMergerProcessor;
import eu.profinit.manta.graphplayground.repository.merger.revision.RevisionRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.revision.RevisionUtils;

/**
 * Tests for standard merger processor.
 *
 * @author tpolacok
 */
public class StandardMergerProcessorTest extends CommonMergerProcessorTest {

    @Override
    protected void beforeEachInit(Double mergedRevision, Double latestCommittedRevision) {
        testGraphUtil = new StandardTestGraphUtil(driver);
        // Init merger processor
        GraphOperation graphOperation = new GraphOperation();
        GraphCreation graphCreation = new GraphCreation(graphOperation);
        RevisionUtils revisionUtils = new RevisionUtils(graphCreation);
        initialProcessor = new StandardMergerProcessor(
                new SuperRootHandler(), new SourceRootHandler(graphOperation), new RevisionRootHandler(),
                graphOperation, graphCreation, revisionUtils);

        testedProcessor = new StandardMergerProcessor(
                new SuperRootHandler(), new SourceRootHandler(graphOperation), new RevisionRootHandler(),
                graphOperation, graphCreation, revisionUtils);

        context = new StandardProcessorContext(mergedRevision, latestCommittedRevision);
    }
}
