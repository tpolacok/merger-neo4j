package eu.profinit.manta.graphplayground.repository.merger.parallel.manager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.stream.IntStream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;

import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.ItemTypes;
import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelNodeQueue;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.EdgeAttributePreprocessor;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.EdgeNodePreprocessor;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.NodeAttributePreprocessor;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.NodePreprocessor;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.phaser.QueuePhaser;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.worker.MergerWorker;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;

/**
 * Parallel merger manager.
 *
 * Deadlocks can happen frequently - processing is retried until finished.
 */
public class RetryParallelMergerManager extends ParallelMergerManager {

    /**
     * @param LOGGER               Logger instance.
     * @param context              Merger context.
     * @param processor            Merger processor.
     * @param graphDatabaseService Database service.
     * @param threads              Number of threads.
     */
    public RetryParallelMergerManager(Log LOGGER, StoredProcessorContext context, StoredMergerProcessor processor,
                                      GraphDatabaseService graphDatabaseService, int threads) {
        super(LOGGER, context, processor, graphDatabaseService, threads);
    }

    /**
     * Merges input graph in parallel.
     * This approach splits merging into 5 parts:
     * <ul>
     *     <li>
     *         Sequential merging of source codes, layers and resources
     *     </li>
     *     <li>
     *         Preprocessing nodes in a way that mapping of parent node to children nodes is created (resources included).
     *         Resource nodes are input into synchronized queue.
     *     </li>
     *     <li>
     *          Nodes are processed in a way that parent has to be processed before its children
     *          - by finishing processing (merging and committing the change) of the children of
     *          given parent node by certain thread, all the subtrees of processed children nodes
     *          can be processed independently. Problem with this solution is that sometimes it
     *          can happen that new node is not only connected by relationship to its parent but
     *          also to certain resource. To prevent database locks or possible deadlocks these
     *          relationships are collected and created only after whole node input tree has been processed.
     *     </li>
     *     <li>
     *         Nodes attributes are split into 2 groups - MAPS_TO and other - each group is split
     *         into thread-containers which is then processed in phases - non-blocking operation are performed as processed,
     *         but potentially blocking operations are stored and processed in next steps. MAPS_TO attributes can be evaluated
     *         into edge creation which is then processed as a part of edge processing.
     *     </li>
     *     <li>
     *         At first edges are split into 2 groups (perspective and other) and are evaluated in non-blocking way, while
     *         creating structures for delayed execution (creation of the edges, node attributes (perspective))
     *         These structures are then are split by node groups so that no group has node intersection with other
     *         group - this is done in more steps to ensure further split of edges and their attributes.
     *     </li>
     * </ul>
     *
     * @param input Merge input.
     * @return Result of the merging.
     */
    @Override
    public ProcessingResult merge(List<List<List<String>>> input) {
        LOGGER.info("Starting merging.");
        ProcessingResult processingResult = initialMerge(input);
        List<ProcessingResult> processingResults = Collections.synchronizedList(new ArrayList<>());

        ExecutorService executorService = Executors.newFixedThreadPool(threads);

        ParallelNodeQueue nodeQueue = NodePreprocessor.preprocess(
                input.get(ItemTypes.RESOURCE.getInputFilePosition()),
                input.get(ItemTypes.NODE.getInputFilePosition())
        );
        LOGGER.info("Done preprocessing nodes.");

        QueuePhaser nodeAttributePhaser = new QueuePhaser(
                NodeAttributePreprocessor.initialSplit(input.get(ItemTypes.NODE_ATTRIBUTE.getInputFilePosition()), threads)
        );
        QueuePhaser edgePhaser = new QueuePhaser(
                EdgeNodePreprocessor.initialSplit(input.get(ItemTypes.EDGE.getInputFilePosition()), threads)
        );

        Map<String, List<CommonMergeQuery>> edgeToAttributes
                = EdgeAttributePreprocessor.preprocessForEdges(input.get(ItemTypes.EDGE_ATTRIBUTE.getInputFilePosition()));
        context.setEdgeToEdgeAttributeMap(edgeToAttributes);

        // Processing phaser
        Phaser phaser = new Phaser(1);

        long start = System.currentTimeMillis();
        IntStream.range(0, threads).forEach(
                e -> executorService.submit(
                        new MergerWorker(
                                graphDatabaseService, processor,
                                nodeQueue, nodeAttributePhaser, edgePhaser,
                                LOGGER, context, phaser,
                                processingResults, nodeLock, e + ""
                        )
                )
        );
        phaser.arriveAndAwaitAdvance();
        phaser.arriveAndAwaitAdvance();
        long end = System.currentTimeMillis();
        LOGGER.info("Deadlock-retry: Done nodes " + (end - start));

        start = System.currentTimeMillis();
        IntStream.range(0, nodeAttributePhaser.phaseSize()).forEach(i -> phaser.arriveAndAwaitAdvance());

        nodeAttributePhaser.nextPhase(
                NodeAttributePreprocessor.redistributeNodeAttributesQueries(context.getNodeAttributeQueryList(), threads)
        );
        phaser.arriveAndAwaitAdvance();

        IntStream.range(0, nodeAttributePhaser.phaseSize()).forEach(i -> phaser.arriveAndAwaitAdvance());

        end = System.currentTimeMillis();
        LOGGER.info("Deadlock-retry: Node attributes " + (end - start));

        start = System.currentTimeMillis();
        IntStream.range(0, edgePhaser.phaseSize()).forEach(i -> phaser.arriveAndAwaitAdvance());

        edgePhaser.nextPhase(
                EdgeNodePreprocessor.splitDelayedEdges(context.getEdgeQueryList(),
                        edgeToAttributes, threads)
        );
        phaser.arriveAndAwaitAdvance();

        IntStream.range(0, edgePhaser.phaseSize()).forEach(i -> phaser.arriveAndAwaitAdvance());
        end = System.currentTimeMillis();
        LOGGER.info("Deadlock-retry: Done edges " + (end - start));

        waitForExecutionFinish(executorService);

        processingResults.forEach(processingResult::add);
        processingResult.setRequestedSourceCodes(context.getRequestedSourceCodes());
        LOGGER.info("Done merging.");
        return processingResult;
    }

}
