package eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import eu.profinit.manta.graphplayground.model.manta.merger.parallel.container.MergeContainer;

/**
 * Common queue for multithreaded processing of merge.
 *
 * @author tpolacok
 */
public class ParallelMergeContainerQueue{

    /**
     * Blocking queue containing available containers of merge objects.
     */
    private final BlockingQueue<MergeContainer> containerQueue = new LinkedBlockingDeque<>();

    /**
     * Initial size of the queue - size of all the containers in the queue.
     */
    private int initialSize = 0;

    /**
     * Constructor - takes in a list of {@link MergeContainer} and inserts them into queue.
     * @param mergeContainers List of {@link MergeContainer}
     */
    public ParallelMergeContainerQueue(List<MergeContainer> mergeContainers) {
        mergeContainers.forEach(container -> this.initialSize += container.getContent().size());
        this.containerQueue.addAll(mergeContainers);
    }


    public int getInitialSize() {
        return initialSize;
    }

    /**
     * @return {@link MergeContainer} or null from the queue.
     */
    public MergeContainer poll() {
        return containerQueue.poll();
    }
}
