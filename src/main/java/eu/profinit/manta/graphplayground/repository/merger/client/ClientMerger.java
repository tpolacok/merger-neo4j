package eu.profinit.manta.graphplayground.repository.merger.client;

import com.google.common.collect.Lists;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerType;
import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult;
import eu.profinit.manta.graphplayground.model.manta.merger.StandardProcessorContext;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.MergerOutput;
import eu.profinit.manta.graphplayground.repository.GraphSessionRepository;
import eu.profinit.manta.graphplayground.repository.merger.connector.GraphOperation;
import eu.profinit.manta.graphplayground.repository.merger.connector.SourceRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.connector.SuperRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.processor.StandardMergerProcessor;
import eu.profinit.manta.graphplayground.repository.merger.revision.RevisionRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.StoredMerger;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Client merger
 * Merges input remotely.
 *
 * @author tpolacok.
 */
@Repository
public class ClientMerger {

    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientMerger.class);

    /**
     * Session repository.
     */
    private GraphSessionRepository<Session, Transaction> sessionRepository;

    /**
     * Handler for super root node.
     */
    private final SuperRootHandler superRootHandler = new SuperRootHandler();

    /**
     * Handler for revision root node.
     */
    private final RevisionRootHandler revisionRootHandler = new RevisionRootHandler();

    /**
     * Handler for source root node.
     */
    private final SourceRootHandler sourceRootHandler = new SourceRootHandler(new GraphOperation());

    /**
     * Standard merger processor.
     */
    private final StandardMergerProcessor mergerProcessor;

    /**
     * @param sessionRepository Database session repository.
     */
    public ClientMerger(GraphSessionRepository<Session, Transaction> sessionRepository, StandardMergerProcessor mergerProcessor) {
        this.sessionRepository = sessionRepository;
        this.mergerProcessor = mergerProcessor;
    }

    /**
     * Initializes database structures.
     * Ensures that the {@link MantaNodeLabel#SUPER_ROOT}, {@link MantaNodeLabel#REVISION_ROOT},
     * and {@link MantaNodeLabel#SOURCE_NODE} exist.
     */
    private void initWorkspace() {
        sessionRepository.getSession().writeTransaction((TransactionWork<String>) tx -> {
            superRootHandler.ensureRootExistance(tx);
            revisionRootHandler.ensureRootExistance(tx);
            sourceRootHandler.ensureRootExistance(tx);
            return null;
        });
    }

    /**
     * Returns latest committed revision number.
     */
    private Double getLatestCommittedRevision() {
        return sessionRepository.getSession().writeTransaction(
                revisionRootHandler::getLatestCommittedRevisionNumber
        );
    }

    /**
     * Initializes context object for {@link MergerType#CLIENT} merger.
     */
    private StandardProcessorContext initContext(Double revision) {
        Double latestCommittedRevision = getLatestCommittedRevision();
        return new StandardProcessorContext(revision, latestCommittedRevision);
    }

    /**
     * Iterates the merge input and merges it into the database.
     * @param mergeObjects List of merge objects.
     * @param revision Merged revision.
     * @return Map of merge result.
     */
    public MergerOutput processStandardMerge(List<List<String>> mergeObjects, Double revision) {
        initWorkspace();
        StandardProcessorContext context = initContext(revision);
        ProcessingResult processingResult = new ProcessingResult();
        long start = System.currentTimeMillis();
        for (List<List<String>> batch: Lists.partition(mergeObjects, StoredMerger.BATCH_SIZE)) {
            sessionRepository.getSession().writeTransaction((TransactionWork<String>) tx -> {
                context.setDbTransaction(tx);
                for (List<String> mergeObject : batch) {
                    long mergeTimeStart = System.currentTimeMillis();
                    List<MergerProcessorResult> mergerResults = mergerProcessor.mergeObject(mergeObject.toArray(new String[0]), context);
                    long mergeTime = System.currentTimeMillis() - mergeTimeStart;
                    if (mergeTime > StoredMerger.NORMAL_MERGE_TIME.toMillis()) {
                        LOGGER.info("Merge time of element {} was {} ms.", ArrayUtils.toString(mergeObject),
                                mergeTime);
                    } else {
                        LOGGER.debug("Merge time of element {} was {} ms.", ArrayUtils.toString(mergeObject),
                                mergeTime);
                    }
                    for (MergerProcessorResult mergerResult : mergerResults) {
                        if (mergerResult.getItemType() != null) {
                            processingResult.saveResult(mergerResult.getResultType(), mergerResult.getItemType());
                        } else {
                            processingResult.increaseUnknownObject();
                        }
                    }
                }
                return null;
            });
        }
        long end = System.currentTimeMillis();
        return new MergerOutput(end - start, processingResult);
    }
}
