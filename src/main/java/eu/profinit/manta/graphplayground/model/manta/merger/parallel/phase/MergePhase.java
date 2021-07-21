package eu.profinit.manta.graphplayground.model.manta.merger.parallel.phase;

import java.util.List;

import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelMergeContainerQueue;

/**
 * Wrapper for list of parallel queues.
 *
 * @author tpolacok.
 */
public class MergePhase {

    /**
     * List of queues containing merge containers.
     */
    private List<ParallelMergeContainerQueue> queueList;

    /**
     * @param queueList List of queues containing merge containers.
     */
    public MergePhase(List<ParallelMergeContainerQueue> queueList) {
        this.queueList = queueList;
    }

    /**
     * @return List of queues.
     */
    public List<ParallelMergeContainerQueue> getQueueList() {
        return queueList;
    }

    /**
     * @return Size of the phase.
     */
    public int size() {
        return queueList.size();
    }
}
