package eu.profinit.manta.graphplayground.repository.stored.merger;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerType;
import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.context.ParallelStoredProcessorContext;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.MergerOutput;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.merger.parallel.manager.GroupedParallelMergerManager;
import eu.profinit.manta.graphplayground.repository.merger.parallel.manager.ParallelMergerManager;
import eu.profinit.manta.graphplayground.repository.merger.parallel.manager.RetryParallelMergerManager;
import eu.profinit.manta.graphplayground.repository.merger.parallel.processor.DelayedParallelStoredMergerProcessor;
import eu.profinit.manta.graphplayground.repository.merger.parallel.processor.ParallelStoredMergerProcessor;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphCreation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphOperation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSourceRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSuperRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionUtils;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import eu.profinit.manta.graphplayground.repository.stored.merger.manager.SequentialMergerManager;

/**
 * Implementation of merger as stored procedure running in parallel.
 *
 * @author tpolacok
 */
public class StoredMerger {
    @Context
    public GraphDatabaseService graphDatabaseService;

    @Context
    public Log LOGGER;

    /**
     * Count of merged object per transaction.
     */
    public static final int BATCH_SIZE = 1000;

    /**
     * Threshold for splitting edges - count of remaining edges to split.
     */
    public static final int EDGE_REMAINING_SPLIT_THRESHOLD = 1000;

    /**
     * Threshold for splitting edges - sum of all groups.
     */
    public static final int EDGE_COUNT_SPLIT_THRESHOLD = 100;

    /**
     * Coeficient to truncate size of largest container when splitting edges.
     */
    public static final double TRUNCATE_COEFICIENT = 1.5;

    /**
     * Normal merge time of the object.
     */
    public static final Duration NORMAL_MERGE_TIME = Duration.ofMillis(1000);

    @Procedure(value = "manta.mergeParallel", mode = Mode.WRITE)
    @Description("Merges input graph into repository in parallel.")
    public Stream<MergerOutput> mergeParallel(@Name("input") List<List<List<String>>> input,
                                              @Name("revision") Double revision,
                                              @Name("threads") Long threads,
                                              @Name("type") String type) {
        MergerType mergerType = MergerType.valueOf(type);
        StoredMergerProcessor processor = getMergerProcessor(mergerType);
        StoredProcessorContext context = getContext(revision, mergerType);
        ParallelMergerManager mergerManager = getMergerManager(mergerType, processor, context, threads.intValue());

        Long start = System.currentTimeMillis();
        ProcessingResult processingResult = mergerManager.merge(input);
        Long end = System.currentTimeMillis();

        return Stream.of(new MergerOutput(end - start, processingResult));
    }

    @Procedure(value = "manta.mergeSequential", mode = Mode.WRITE)
    @Description("Merges input graph into repository in serial.")
    public Stream<MergerOutput> mergeSequential(@Name("input") List<List<List<String>>> input,
                                                @Name("revision") Double revision) {
        StoredMergerProcessor processor = getMergerProcessor(MergerType.SERVER_SEQUENTIAL);
        StoredProcessorContext context = getContext(revision, MergerType.SERVER_SEQUENTIAL);
        Long start = System.currentTimeMillis();

        SequentialMergerManager mergerManager
                = new SequentialMergerManager(LOGGER, context, processor, graphDatabaseService);
        ProcessingResult processingResult = mergerManager.merge(input);

        Long end = System.currentTimeMillis();
        return Stream.of(new MergerOutput(end - start, processingResult));
    }

    private ParallelMergerManager getMergerManager(MergerType mergerType, StoredMergerProcessor processor,
                                                   StoredProcessorContext context, int threads) {
        switch (mergerType) {
            case SERVER_PARALLEL_A: {
                return new RetryParallelMergerManager(LOGGER, context, processor, graphDatabaseService, threads);
            }
            case SERVER_PARALLEL_B: {
                return new GroupedParallelMergerManager(LOGGER, context, processor, graphDatabaseService, threads);
            }
            default: throw new IllegalArgumentException("Invalid merger type.");
        }
    }

    /**
     * Initializes merge context.
     * @param revision Merged revision.
     * @return Initialized merge context.
     */
    private StoredProcessorContext getContext(Double revision, MergerType type) {
        double latestCommittedRevision = getLatestCommittedRevision();
        switch(type) {
            case SERVER_SEQUENTIAL:
                return new StoredProcessorContext(revision, latestCommittedRevision);
            case SERVER_PARALLEL_A:
            case SERVER_PARALLEL_B:
                return new ParallelStoredProcessorContext(revision, latestCommittedRevision);
            default: throw new InvalidArgumentException(String.format("Invalid type %s", type));
        }
    }

    /**
     * Initializes merger processor
     * @param type Type of merger processor.
     * @return Initialized merger processor.
     */
    private StoredMergerProcessor getMergerProcessor(MergerType type) {
        StoredSuperRootHandler superRootHandler = new StoredSuperRootHandler();
        StoredRevisionRootHandler revisionRootHandler = new StoredRevisionRootHandler();
        StoredRevisionUtils revisionUtils = new StoredRevisionUtils();
        StoredGraphOperation graphOperation = new StoredGraphOperation(revisionUtils);
        StoredGraphCreation graphCreation = new StoredGraphCreation(graphOperation);
        StoredSourceRootHandler sourceRootHandler = new StoredSourceRootHandler(graphOperation);
        ensureDatabaseStructuresExistence(superRootHandler, sourceRootHandler, revisionRootHandler);
        switch(type) {
            case SERVER_SEQUENTIAL:
                return new StoredMergerProcessor(superRootHandler, sourceRootHandler,
                        revisionRootHandler, graphOperation, graphCreation, revisionUtils, LOGGER);
            case SERVER_PARALLEL_A:
                return new DelayedParallelStoredMergerProcessor(superRootHandler, sourceRootHandler,
                        revisionRootHandler, graphOperation, graphCreation, revisionUtils, LOGGER);
            case SERVER_PARALLEL_B:
                return new ParallelStoredMergerProcessor(superRootHandler, sourceRootHandler,
                        revisionRootHandler, graphOperation, graphCreation, revisionUtils, LOGGER);
            default: throw new InvalidArgumentException(String.format("Invalid type %s", type));
        }
    }

    /**
     * Ensures required database structures exist.
     * @param superRootHandler Super root handler.
     * @param sourceRootHandler Source root handler.
     * @param revisionRootHandler Revision root handler.
     */
    private  void ensureDatabaseStructuresExistence(StoredSuperRootHandler superRootHandler,
                                                    StoredSourceRootHandler sourceRootHandler,
                                                    StoredRevisionRootHandler revisionRootHandler) {
        Transaction tx = graphDatabaseService.beginTx();
        superRootHandler.ensureRootExistance(tx);
        revisionRootHandler.ensureRootExistance(tx);
        sourceRootHandler.ensureRootExistance(tx);
        tx.commit();
    }

    /**
     * Retrieves latest committed revision number.
     */
    private double getLatestCommittedRevision() {
        Transaction tx = graphDatabaseService.beginTx();
        double latestCommittedRevision = new StoredRevisionRootHandler().getLatestCommittedRevisionNumber(tx);
        tx.commit();
        return latestCommittedRevision;

    }

    /**
     * Process results from merged object.
     * @param processingResult Final result object.
     * @param mergerResults Results from merging object.
     */
    public static void processResults(ProcessingResult processingResult, List<MergerProcessorResult> mergerResults) {
        for (MergerProcessorResult mergerResult : mergerResults) {
            if (mergerResult.getItemType() != null) {
                processingResult.saveResult(mergerResult.getResultType(), mergerResult.getItemType());
            } else {
                processingResult.increaseUnknownObject();
            }
        }
    }

}
