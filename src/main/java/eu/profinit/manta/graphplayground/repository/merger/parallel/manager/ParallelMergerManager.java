package eu.profinit.manta.graphplayground.repository.merger.parallel.manager;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.lock.NodeLock;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import eu.profinit.manta.graphplayground.repository.merger.parallel.query.RawMergeQuery;
import eu.profinit.manta.graphplayground.repository.stored.merger.StoredMerger;
import eu.profinit.manta.graphplayground.repository.stored.merger.manager.StoredMergerManager;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;

/**
 * Parallel merger manager.
 * Processes input in parallel.
 *
 * @author tpolacok
 */
public abstract class ParallelMergerManager extends StoredMergerManager {

    /**
     * Number of threads.
     */
    protected final int threads;

    /**
     * Lock for the node processing.
     */
    protected final NodeLock nodeLock;

    /**
     *
     * @param LOGGER Logger instance.
     * @param context Merger context.
     * @param processor Merger processor.
     * @param graphDatabaseService Database service.
     * @param threads Number of threads.
     */
    public ParallelMergerManager(Log LOGGER, StoredProcessorContext context, StoredMergerProcessor processor,
                                 GraphDatabaseService graphDatabaseService, int threads) {
        super(LOGGER, context, processor, graphDatabaseService);
        this.threads = threads;
        this.nodeLock = new NodeLock(threads);
    }

    /**
     * Waits for the execution of all processes submitted to the executor service.
     * @param executorService Executor service instance.
     */
    protected void waitForExecutionFinish(ExecutorService executorService) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    /**
     * Merges initial structures - source codes, layers, resources.
     * @param input List of merge object groups.
     * @return Processing result.
     */
    protected ProcessingResult initialMerge(List<List<List<String>>> input) {
        ProcessingResult processingResult = mergeStart(
                input.subList(
                        DataflowObjectFormats.ItemTypes.SOURCE_CODE.getInputFilePosition(),
                        DataflowObjectFormats.ItemTypes.NODE.getInputFilePosition()
                )
        );
        LOGGER.info("Done processing source codes, layers and resources.");
        return processingResult;
    }

    /**
     * Processes first part of the merge sequentially - source codes, layers and resources.
     * @param mergeObjectsGroups Groups of merge objects to process
     * @return Processing result.
     */
    protected ProcessingResult mergeStart(List<List<List<String>>> mergeObjectsGroups) {
        ProcessingResult processingResult = new ProcessingResult();
        Transaction transaction = graphDatabaseService.beginTx();
        context.setServerDbTransaction(transaction);
        for (List<List<String>> mergeObjectsGroup: mergeObjectsGroups) {
            for (List<String> mergeObject : mergeObjectsGroup) {
                List<MergerProcessorResult> mergerResult = mergeObject(new RawMergeQuery(mergeObject), processor, context, LOGGER);
                StoredMerger.processResults(processingResult, mergerResult);
            }
        }
        transaction.commit();
        return processingResult;
    }

}
