package eu.profinit.manta.graphplayground.repository.stored.merger.manager;

import java.util.ArrayList;
import java.util.List;

import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.stored.merger.StoredMerger;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.logging.Log;

import com.google.common.collect.Lists;

import eu.profinit.manta.graphplayground.repository.merger.parallel.query.RawMergeQuery;

/**
 * Sequential merger manager.
 * Processes input sequentially.
 *
 * @author tpolacok.
 */
public class SequentialMergerManager extends StoredMergerManager {

    /**
     * @param LOGGER               Logger instance.
     * @param context              Processor context.
     * @param processor            Merger processor.
     * @param graphDatabaseService Database service.
     */
    public SequentialMergerManager(Log LOGGER, StoredProcessorContext context, StoredMergerProcessor processor, GraphDatabaseService graphDatabaseService) {
        super(LOGGER, context, processor, graphDatabaseService);
    }

    /**
     * Merges input sequentially.
     * @param input Merging input split into groups.
     * @return Processing result.
     */
    @Override
    public ProcessingResult merge(List<List<List<String>>> input) {
        LOGGER.info("Starting merging.");
        ProcessingResult processingResult = new ProcessingResult();
        List<Long> times = new ArrayList<>();
        for (List<List<String>> mergeObjectTypeGroup: input) {
            times.add(System.currentTimeMillis());
            for (List<List<String>> mergeBatch: Lists.partition(mergeObjectTypeGroup, StoredMerger.BATCH_SIZE)) {
                Transaction transaction = graphDatabaseService.beginTx();
                context.setServerDbTransaction(transaction);
                try {
                    for (List<String> mergeObject : mergeBatch) {
                        List<MergerProcessorResult> mergerResults = mergeObject(new RawMergeQuery(mergeObject),
                                processor, context, LOGGER);
                        StoredMerger.processResults(processingResult, mergerResults);
                    }
                    transaction.commit();
                } catch (DeadlockDetectedException e) {
                    LOGGER.warn("Transaction failed." + e.getMessage());
                    retry(mergeBatch);
                }
            }
        }
        long end = System.currentTimeMillis();
        LOGGER.info("Sequential: Done source codes, layers, relationships" + (times.get(3) - times.get(0)));
        LOGGER.info("Sequential: Done all nodes and node attributes" + (times.get(5) - times.get(3)));
        LOGGER.info("Sequential: Done all edges and edge attributes" + (end - times.get(5)));
        processingResult.setRequestedSourceCodes(context.getRequestedSourceCodes());
        return processingResult;
    }

    /**
     * Merges objects until no deadlock exception is thrown.
     *
     * @param mergeObjects List of merge objects.
     */
    private void retry(List<List<String>> mergeObjects) {
        try {
            Transaction transaction = graphDatabaseService.beginTx();
            context.setServerDbTransaction(transaction);
            for (List<String> mergeObject: mergeObjects) {
                processor.mergeObject(mergeObject.toArray(new String[0]), context);
            }
            transaction.commit();
        } catch (DeadlockDetectedException e) {
            LOGGER.warn("Transaction failed again." + e.getMessage());
            retry(mergeObjects);
        }
    }

}
