package eu.profinit.manta.graphplayground.repository.merger.parallel.worker;

import com.google.common.collect.Lists;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.ItemTypes;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult;
import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult.ResultType;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.container.MergeContainer;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.context.ParallelStoredProcessorContext;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.lock.NodeLock;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelMergeContainerQueue;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelNodeQueue;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.state.NodeMergingState;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.state.NodeMergingStep;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.merger.parallel.manager.ParallelMergerManager;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.phaser.QueuePhaser;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedNodeResourceEdgeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.RawMergeQuery;
import eu.profinit.manta.graphplayground.repository.stored.merger.StoredMerger;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.logging.Log;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Phaser;

/**
 * Merging worker thread.
 *
 * This merging thread processes
 * <ul>
 *     <li>
 *         Nodes - shared node queue - node processing means processing all node's children.
 *         After queue processing is done, worker tries to retrieve node lock and finish processing
 *         remaining nodes (nodes with different resource than parent).
 *     </li>
 *     <li>
 *         Node attributes - iterates shared list of queues for which one element is retrieved
 *         and processed per every list and then awaits at the barrier.
 *     </li>
 *     <li>
 *         Edges and edge attributes - iterates shared list of queues for which one element is retrieved
 *         and processed per every list and then awaits at the barrier.
 *     </li>
 * </ul>
 *
 * @author tpolacok
 */
public class MergerWorker implements Runnable {

    /**
     * Database service for creating transactions.
     */
    protected GraphDatabaseService graphDatabaseService;

    /**
     * Merging processor.
     */
    protected StoredMergerProcessor processor;

    /**
     * Contextual object.
     */
    protected ParallelStoredProcessorContext context;

    /**
     * Synchronized node queue for node processing.
     */
    protected ParallelNodeQueue nodeQueue;

    /**
     * Phase manager for node attributes.
     */
    protected QueuePhaser nodeAttributeQueuePhaser;

    /**
     * Phase manager for edges.
     */
    protected QueuePhaser edgeQueuePhaser;

    /**
     * Lock for node processing.
     */
    protected NodeLock nodeLock;

    /**
     * Merging phaser.
     */
    protected Phaser phaser;

    /**
     * Processing result for merger.
     */
    protected ProcessingResult processingResult;

    /**
     * Synchronized list of processing results.
     */
    protected List<ProcessingResult> sharedResult;

    /**
     * Logger object.
     */
    protected Log LOGGER;

    /**
     * Thread name.
     */
    protected String name;

    /**
     * TODO TBD - synchronization for incremental updates (currently only remove-all subtree) - need to lock given node
     * TODO and commit all other threads.
     * @param graphDatabaseService Graph service.
     * @param processor Merger processor.
     * @param nodeQueue Queue for nodes.
     * @param nodeAttributeQueuePhaser Phase manager for node attributes.
     * @param edgeQueuePhaser Phase manager for edges.
     * @param LOGGER LOGGER instance.
     * @param context Contextual object.
     * @param phaser Phaser for merging phases.
     * @param sharedResult Synchronized list of results
     * @param nodeLock Lock for nodes.
     * @param name Thread name.
     */
    public MergerWorker(GraphDatabaseService graphDatabaseService, StoredMergerProcessor processor,
                        ParallelNodeQueue nodeQueue, QueuePhaser nodeAttributeQueuePhaser,
                        QueuePhaser edgeQueuePhaser,
                        Log LOGGER, StoredProcessorContext context, Phaser phaser,
                        List<ProcessingResult> sharedResult, NodeLock nodeLock, String name) {
        this.graphDatabaseService = graphDatabaseService;
        this.processor = processor;
        this.context = new ParallelStoredProcessorContext(context);
        this.nodeQueue = nodeQueue;
        this.nodeAttributeQueuePhaser = nodeAttributeQueuePhaser;
        this.edgeQueuePhaser = edgeQueuePhaser;
        this.phaser = phaser;
        this.LOGGER = LOGGER;
        this.processingResult = new ProcessingResult();
        this.sharedResult = sharedResult;
        this.nodeLock = nodeLock;
        this.name = name;
        this.phaser.register();
    }

    @Override
    public void run() {
        try {
            processNodes();
            processPhases(nodeAttributeQueuePhaser);
            processPhases(edgeQueuePhaser);
            sharedResult.add(processingResult);
        } catch (Exception e) {
            workerShutdown();
        }
    }

    /**
     * Merges nodes.
     * Nodes to be processed are placed inside shared queue.
     * Each thread also has internal queue, which has priority - if no element is in internal queue,
     * shared queue is queried. If no element is inside shared queue thread is put to sleep and waits for work.
     *xx
     * Processing of given node means merging all its children (and attributes possibly). After that all children are put
     * to internal queue. After each node's processing threads checks for waiting threads and
     * transfers part of its internal queue to shared queue and wakes up sleeping threads.
     *
     * If all internal queues are empty as well as shared queue last threads wakes all waiting threads and finishes this phase.
     */
    protected void processNodes() {
        Queue<String> internalQueue = new ArrayDeque<>();
        Transaction tx = graphDatabaseService.beginTx();
        context.setServerDbTransaction(tx);
        List<List<String>> journal = new ArrayList<>();
        String nodeId;
        while (true) {
            NodeMergingStep step = getNextStep(internalQueue);
            if (step.getState() == NodeMergingState.PROCESSING_CONTINUE) {
                continue;
            } else if (step.getState() == NodeMergingState.PROCESSING_DONE) {
                break;
            } else {
                nodeId = step.getNodeId();
            }
            List<List<String>> ownedNodes = nodeQueue.getOwned(nodeId);
            if (!ownedNodes.isEmpty()) {
                mergeNodesAndAttributes(ownedNodes, journal, internalQueue);
            }

            if (nodeLock.getWaitingCounter() > 0 && internalQueue.size() > nodeLock.getWaitingCounter()){
                splitInternalQueue(internalQueue, journal);
            }
        }
        ensureTransactionCommit(journal);
        phaser.arriveAndAwaitAdvance();
        processDelayedNodeObjects();
        phaser.arriveAndAwaitAdvance();
    }

    /**
     * Gets next step of node processing.
     * Either new node is retrieved or work is terminated or thread continues to wait.
     * @param internalQueue Internal queue of thread.
     * @return Merging step.
     */
    private NodeMergingStep getNextStep(Queue<String> internalQueue) {
        String nodeId;
        if (!internalQueue.isEmpty()) {
            nodeId = internalQueue.poll();
        } else {
            nodeId = nodeQueue.poll();
            if (nodeId == null) {
                try {
                    nodeLock.getWaitLock().lock();
                    nodeLock.increaseWaitingCounter();
                    if (nodeLock.getThreads() == nodeLock.getWaitingCounter()) {
                        nodeLock.getWaitingForWorkCondition().signalAll();
                        return NodeMergingStep.of(NodeMergingState.PROCESSING_DONE);
                    } else {
                        nodeLock.getWaitingForWorkCondition().await();
                    }
                    if (nodeLock.getThreads() == nodeLock.getWaitingCounter()) {
                        return NodeMergingStep.of(NodeMergingState.PROCESSING_DONE);
                    }
                    nodeLock.decreaseWaitingCounter();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    nodeLock.getWaitLock().unlock();
                }
                return NodeMergingStep.of(NodeMergingState.PROCESSING_CONTINUE);
            }
        }
        return NodeMergingStep.of(NodeMergingState.PROCESSING_NODE, nodeId);
    }

    /**
     * Merges given objects and inserts processed elements into the journal.
     * @param ownedNodes Objects to be processed.
     * @param journal Journal of processed objects.
     * @param internalQueue Internal queue of objects.
     */
    private void mergeNodesAndAttributes(List<List<String>> ownedNodes, List<List<String>> journal,
                                         Queue<String> internalQueue) {
        for (List<String> ownedNode : ownedNodes) {
            journal.add(ownedNode);
            List<MergerProcessorResult> mergerResult;
            try {
                mergerResult = ParallelMergerManager.mergeObject(new RawMergeQuery(ownedNode), processor, context, LOGGER);
            } catch (DeadlockDetectedException e) {
                LOGGER.warn(name + "nodes merging failed: " + e.getMessage());
                mergerResult = retryNodesMerge(journal);
            }
            nodePostProcessing(internalQueue, ownedNode, mergerResult.get(0).getResultType());
            StoredMerger.processResults(processingResult, mergerResult);
        }
    }

    /**
     * Tries to commit transaction, in case of deadlock exception journal is retried.
     * @param journal List of merge objects.
     */
    private void ensureTransactionCommit(List<List<String>> journal) {
        Transaction tx = context.getServerDbTransaction();
        try {
            tx.commit();
        } catch (DeadlockDetectedException e) {
            LOGGER.warn(name + " nodes merging failed: " + e.getMessage());
            retryNodesMerge(journal);
        }
    }

    /**
     * Retries merging of journal
     * @param journal Merge objects
     * @return Merging processor result of last object.
     */
    private List<MergerProcessorResult> retryNodesMerge(List<List<String>> journal) {
        Transaction tx = graphDatabaseService.beginTx();
        context.setServerDbTransaction(tx);
        List<MergerProcessorResult> mergerResult = null;
        try {
            for (List<String> mergeObject : journal) {
                mergerResult = ParallelMergerManager.mergeObject(new RawMergeQuery(mergeObject), processor, context, LOGGER);
            }
            tx.commit();
        } catch (DeadlockDetectedException e) {
            LOGGER.warn(name + " nodes merging failed: " + e.getMessage());
            mergerResult = retryNodesMerge(journal);
        }
        return mergerResult;
    }

    /**
     * Moves elements from internal queue to synchronized queue by the numbers of waiting threads.
     * @param internalQueue Internal merge queue.
     * @param journal Merged objects.
     */
    private void splitInternalQueue(Queue<String> internalQueue, List<List<String>> journal) {
        ensureTransactionCommit(journal);
        Transaction tx = graphDatabaseService.beginTx();
        context.setServerDbTransaction(tx);
        int keep = internalQueue.size() / (nodeLock.getWaitingCounter() + 1);
        for (int i = 0; i < internalQueue.size() - keep; ++i) {
            nodeQueue.addToQueue(internalQueue.poll());
        }
        nodeLock.getWaitLock().lock();
        nodeLock.getWaitingForWorkCondition().signalAll();
        nodeLock.getWaitLock().unlock();
    }

    /**
     * Adds necessary node-resource relationships
     * @param mergeObject Merged object.
     * @param resultType Result type.
     */
    private void nodePostProcessing(Queue<String> internalQueue,
                                    List<String> mergeObject, ResultType resultType) {
        ItemTypes itemType = ItemTypes.parseString(mergeObject.get(0));
        if (itemType.equals(ItemTypes.NODE)
                && nodeQueue.differentResource(mergeObject.get(1))
                && resultType.equals(ResultType.NEW_OBJECT)) {
            nodeQueue.addResourceProcessing(
                    new DelayedNodeResourceEdgeQuery(
                            mergeObject.get(1),
                            mergeObject.get(5)
                    )
            );
        }
        if (itemType.equals(ItemTypes.NODE)) {
            internalQueue.add(mergeObject.get(1));
        }
    }

    /**
     * Processes node resources.
     *
     * Only one thread is allowed to process these requests.
     */
    protected void processDelayedNodeObjects() {
        try {
            nodeLock.getWaitLock().lock();
            CommonMergeQuery query = nodeQueue.getNextDelayedMergeQuery();
            if (query == null) return;
            Transaction tx = graphDatabaseService.beginTx();
            context.setServerDbTransaction(tx);
            while (query != null) {
                query.evaluate(processor, context);
                query = nodeQueue.getNextDelayedMergeQuery();
            }
            tx.commit();
        } finally {
            nodeLock.getWaitLock().unlock();
        }
    }

    /**
     * Process phases of phaser.
     * Attributes and edges have 2 main phases - non-blocking, blockin.
     * @param queuePhaser Queue phaser.
     */
    protected void processPhases(QueuePhaser queuePhaser) {
        queuePhaser.getCurrentPhase().getQueueList().forEach(this::processMergeQueue);
        phaser.arriveAndAwaitAdvance();
        queuePhaser.getCurrentPhase().getQueueList().forEach(this::processMergeQueue);
    }

    /**
     * Processes merge objects.
     * @param mergeQueue Queue containing merge containers to process.
     */
    protected void processMergeQueue(ParallelMergeContainerQueue mergeQueue) {
        MergeContainer container = mergeQueue.poll();
        if (container != null && !container.getContent().isEmpty()) {
            mergeObjectsToDatabase(container.getContent());
        }
        phaser.arriveAndAwaitAdvance();
    }

    /**
     * Merging of objects - transaction is committed after processing MERGE_COUNT objects
     * to prevent long locking and long deadlock retry attempts.
     * @param mergeObjects List of merged objects.
     */
    private void mergeObjectsToDatabase(List<CommonMergeQuery> mergeObjects) {
        for (List<CommonMergeQuery> mergeBatch: Lists.partition(mergeObjects, StoredMerger.BATCH_SIZE)) {
            try {
                Transaction tx = graphDatabaseService.beginTx();
                context.setServerDbTransaction(tx);
                for (CommonMergeQuery mergeObject : mergeBatch) {
                    try {
                        List<MergerProcessorResult> mergerResult
                                = ParallelMergerManager.mergeObject(mergeObject, processor, context, LOGGER);
                        StoredMerger.processResults(processingResult, mergerResult);
                    } catch (Exception e) {
                        LOGGER.info(e.toString());
                        throw e;
                    }
                }
                tx.commit();
            } catch (DeadlockDetectedException e) {
                LOGGER.warn(name + " failed: " + e.getMessage());
                mergeObjectsToDatabase(mergeBatch);
            }
        }
    }

    /**
     * Synchronizes failure of the merging worker.
     *
     * Node processing is synchronized.
     * Worker is deregistered from the phasers.
     */
    protected void workerShutdown() {
        LOGGER.error("Exception occurred within a merging thread. Unregistering from all phases.");
        if (phaser.getPhase() == 0) {
            nodeLock.getWaitLock().lock();
            nodeLock.failedThread();
            if (nodeLock.getThreads() == nodeLock.getWaitingCounter()) {
                nodeLock.getWaitingForWorkCondition().signalAll();
            }
            nodeLock.getWaitLock().unlock();
        }
        phaser.arriveAndDeregister();
    }
}
