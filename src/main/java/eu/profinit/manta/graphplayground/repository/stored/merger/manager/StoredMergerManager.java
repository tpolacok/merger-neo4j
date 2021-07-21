package eu.profinit.manta.graphplayground.repository.stored.merger.manager;

import java.util.List;

import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.stored.merger.StoredMerger;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;

import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;

/**
 * Abstract merger manager.
 * Defines common structures used for merging.
 *
 * @author tpolacok
 */
public abstract class StoredMergerManager {

    /**
     * Logger instance.
     */
    protected final Log LOGGER;

    /**
     * Processor context.
     */
    protected final StoredProcessorContext context;

    /**
     * Merger processor.
     */
    protected final StoredMergerProcessor processor;

    /**
     * Database service.
     */
    protected final GraphDatabaseService graphDatabaseService;

    /**
     * @param LOGGER Logger instance.
     * @param context Processor context.
     * @param processor Merger processor.
     * @param graphDatabaseService Database service.
     */
    public StoredMergerManager(Log LOGGER, StoredProcessorContext context, StoredMergerProcessor processor,
                                   GraphDatabaseService graphDatabaseService) {
        this.LOGGER = LOGGER;
        this.context = context;
        this.processor = processor;
        this.graphDatabaseService = graphDatabaseService;
    }

    /**
     * Merges input.
     * @param input Merging input split into groups.
     * @return Processing result.
     */
    public abstract ProcessingResult merge(List<List<List<String>>> input);

    /**
     * Merges object into the database and logs the merge time.
     * @param mergeObject Merge object.
     * @param processor Processor merging the object.
     * @param context Context for the processor.
     * @param LOGGER Logger.
     * @return List of processing results.
     */
    public static List<MergerProcessorResult> mergeObject(CommonMergeQuery mergeObject, StoredMergerProcessor processor,
                                                          StoredProcessorContext context, Log LOGGER) {
        long mergeTimeStart = System.currentTimeMillis();
        List<MergerProcessorResult> mergerResults =
                mergeObject.evaluate(processor, context);
        long mergeTime = System.currentTimeMillis() - mergeTimeStart;
        if (mergeTime > StoredMerger.NORMAL_MERGE_TIME.toMillis()) {
            LOGGER.info("Merge time of element {} was {} ms.", mergeObject, mergeTime);
        } else {
            LOGGER.debug("Merge time of element {} was {} ms.", mergeObject, mergeTime);
        }
        return mergerResults;
    }
}
