package eu.profinit.manta.graphplayground.repository.merger.parallel.worker;

import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.lock.NodeLock;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelNodeWithAttributesQueue;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.merger.parallel.manager.ParallelMergerManager;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.phaser.QueuePhaser;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.RawMergeQuery;
import eu.profinit.manta.graphplayground.repository.stored.merger.StoredMerger;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.concurrent.Phaser;

/**
 * Merging worker thread.
 *
 * This merging thread processes
 * <ul>
 *     <li>
 *         Nodes and node attributes - shared node queue - node processing means processing all node's children.
 *         For each node also its attributes are processed.
 *         After queue processing is done, worker tries to retrieve node lock and finish processing
 *         remaining nodes (nodes with different resource than parent) and node attributes (MAPS_TO attributes).
 *     </li>
 *     <li>
 *         Edges and edge attributes - iterates shared list of queues for which one element is retrieved
 *         and processed per every list and then awaits at the barrier.
 *     </li>
 * </ul>
 *
 * @author tpolacok
 */
public class GroupedMergerWorker extends MergerWorker {

    /**
     * Node queue with attributes included.
     */
    private final ParallelNodeWithAttributesQueue nodeWithAttributesQueue;

    /**
     * Edge preprocessing phaser.
     */
    private final Phaser edgePhaser;


    /**
     *
     * @param graphDatabaseService Graph service.
     * @param processor Merger processor.
     * @param nodeWithAttributesQueue Queue for nodes and attributes.
     * @param edgeQueuePhaser Edge queue phaser.
     * @param LOGGER LOGGER instance.
     * @param context Contextual object.
     * @param phaser Phaser for merging phases.
     * @param edgePhaser Phaser for edge preprocessing.
     * @param sharedResult Synchronized list of results
     * @param nodeLock Lock for nodes.
     * @param name Thread name.
     */
    public GroupedMergerWorker(GraphDatabaseService graphDatabaseService, StoredMergerProcessor processor,
                               ParallelNodeWithAttributesQueue nodeWithAttributesQueue,
                               QueuePhaser edgeQueuePhaser, Log LOGGER,
                               StoredProcessorContext context, Phaser phaser, Phaser edgePhaser,
                               List<ProcessingResult> sharedResult, NodeLock nodeLock, String name) {
        super(graphDatabaseService, processor, nodeWithAttributesQueue, null, edgeQueuePhaser,
                LOGGER, context, phaser, sharedResult, nodeLock, name);
        this.nodeWithAttributesQueue = nodeWithAttributesQueue;
        this.edgePhaser = edgePhaser;
        edgePhaser.register();
    }

    @Override
    public void run() {
        try {
            processNodes();
            edgePhaser.arriveAndAwaitAdvance();
            processPhases(edgeQueuePhaser);
            sharedResult.add(processingResult);
        } catch (Exception e) {
            workerShutdown();
        }
    }

    /**
     * Overrides parent's method.
     *
     * Not only adds node-resource relationship as parent but also merges MAPS_TO attributes.
     *
     * Only one thread is allowed to process these requests.
     */
    @Override
    protected void processDelayedNodeObjects() {
        try {
            nodeLock.getWaitLock().lock();
            CommonMergeQuery query = nodeQueue.getNextDelayedMergeQuery();
            List<List<String>> mapsToNodeAttributes = nodeWithAttributesQueue.getMapsToAttributes();
            if (query == null && mapsToNodeAttributes.isEmpty()) return;
            Transaction tx = graphDatabaseService.beginTx();
            context.setServerDbTransaction(tx);
            // Adds node-resource relationship
            while (query != null) {
                query.evaluate(processor, context);
                query = nodeQueue.getNextDelayedMergeQuery();
            }
            for (List<String> attribute: mapsToNodeAttributes) {
                List<MergerProcessorResult> mergerResult
                        = ParallelMergerManager.mergeObject(new RawMergeQuery(attribute), processor, context, LOGGER);
                StoredMerger.processResults(processingResult, mergerResult);
            }
            tx.commit();
            nodeWithAttributesQueue.getMapsToAttributes().clear();
        } finally {
            nodeLock.getWaitLock().unlock();
        }
    }

    @Override
    protected void processPhases(QueuePhaser queuePhaser) {
        queuePhaser.getCurrentPhase().getQueueList().forEach(this::processMergeQueue);
    }

    @Override
    protected void workerShutdown() {
        super.workerShutdown();
        edgePhaser.arriveAndDeregister();
    }
}

