package eu.profinit.manta.graphplayground.repository.merger.parallel.manager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.stream.IntStream;

import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats;
import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.phase.MergePhase;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelMergeContainerQueue;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelNodeQueue;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelNodeWithAttributesQueue;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.phaser.QueuePhaser;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.worker.EdgeNeighborhoodPreprocessWorker;
import eu.profinit.manta.graphplayground.repository.merger.parallel.worker.GroupedMergerWorker;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;

import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.NodeAttributePreprocessor;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.NodePreprocessor;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;

/**
 * Parallel merger manager.
 *
 * Node attributes are added to node processing so that locking is minimalized.
 * Edges are split in a way that deadlock should not happen.
 */
public class GroupedParallelMergerManager extends ParallelMergerManager {

    /**
     * @param LOGGER               Logger instance.
     * @param context              Merger context.
     * @param processor            Merger processor.
     * @param graphDatabaseService Database service.
     * @param threads              Number of threads.
     */
    public GroupedParallelMergerManager(Log LOGGER, StoredProcessorContext context, StoredMergerProcessor processor,
                                        GraphDatabaseService graphDatabaseService, int threads) {
        super(LOGGER, context, processor, graphDatabaseService, threads);
    }

    /**
     * Merges input graph in parallel.
     * This approach splits merging into 4 parts:
     * <ul>
     *     <li>
     *         Sequential merging of source codes, layers and resources
     *     </li>
     *     <li>
     *         Preprocessing nodes in a way that mapping of parent node to its attributes and
     *         children nodes is created (resources included).
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
     *          Here, node attributes are processed as a part of node's children.
     *     </li>
     *     <li>
     *         Edges and their attributes are split by node groups so that between nodes of every 2 groups is always
     *         atleast one bordering node to prevent deadlocks - this is also done recursively to ensure further split
     *         of edges and their attributes.
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

        ExecutorService executorService = Executors.newFixedThreadPool(threads + 1);

        ParallelNodeQueue nodeQueue = NodePreprocessor.preprocess(
                input.get(DataflowObjectFormats.ItemTypes.RESOURCE.getInputFilePosition()),
                input.get(DataflowObjectFormats.ItemTypes.NODE.getInputFilePosition())
        );
        ParallelNodeWithAttributesQueue nodeWithAttributesQueue = NodeAttributePreprocessor.preprocessWithNodes(
                nodeQueue, input.get(DataflowObjectFormats.ItemTypes.NODE_ATTRIBUTE.getInputFilePosition()));

        LOGGER.info("Done preprocessing nodes and attributes.");

        QueuePhaser edgeQueuePhaser = new QueuePhaser();
        List<ParallelMergeContainerQueue> edgeQueues = new ArrayList<>();
        Phaser edgePhaser = submitEdgePreprocessor(executorService, input, edgeQueues);

        // Phaser barrier
        Phaser phaser = new Phaser(1);

        IntStream.range(0, threads).forEach(
                e -> executorService.submit(
                        new GroupedMergerWorker(
                                graphDatabaseService, processor,
                                nodeWithAttributesQueue, edgeQueuePhaser, LOGGER, context, phaser,
                                edgePhaser, processingResults, nodeLock, e + ""
                        )
                )
        );
        long start = System.currentTimeMillis();

        // Node barrier
        phaser.arriveAndAwaitAdvance();
        // Node resource barrier
        phaser.arriveAndAwaitAdvance();
        long end = System.currentTimeMillis();
        LOGGER.info("Deadlock-free: Done nodes and node attributes " + (end - start));

        edgeQueuePhaser.nextPhase(new MergePhase(edgeQueues));
        edgePhaser.arriveAndAwaitAdvance();

        start = System.currentTimeMillis();
        IntStream.range(0, edgeQueuePhaser.phaseSize()).forEach(i -> phaser.arriveAndAwaitAdvance());
        end = System.currentTimeMillis();
        LOGGER.info("Deadlock-free: Done edges " + (end - start));

        waitForExecutionFinish(executorService);

        processingResults.forEach(processingResult::add);
        processingResult.setRequestedSourceCodes(context.getRequestedSourceCodes());
        LOGGER.info("Done merging.");
        return processingResult;
    }

    private Phaser submitEdgePreprocessor(ExecutorService executorService, List<List<List<String>>> input,
                                          List<ParallelMergeContainerQueue> edgeQueues) {
        Phaser edgePhaser = new Phaser(1);
        executorService.submit(new EdgeNeighborhoodPreprocessWorker(
                input.get(DataflowObjectFormats.ItemTypes.EDGE.getInputFilePosition()),
                input.get(DataflowObjectFormats.ItemTypes.NODE.getInputFilePosition()),
                input.get(DataflowObjectFormats.ItemTypes.EDGE_ATTRIBUTE.getInputFilePosition()),
                edgeQueues, edgePhaser, threads, LOGGER)
        );
        return edgePhaser;
    }
}
