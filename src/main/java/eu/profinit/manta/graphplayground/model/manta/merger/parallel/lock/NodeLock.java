package eu.profinit.manta.graphplayground.model.manta.merger.parallel.lock;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Lock and lock-condition for node processing.
 * Used for when merging thread is out of work and is put to sleep.
 * Used for when sequential processing the node part is required.
 *
 * @author tpolacok.
 */
public class NodeLock {

    /**
     * Number of waiting threads.
     */
    private int waitingCounter;

    /**
     * Total number of threads.
     */
    private int threads;

    /**
     * Lock instance.
     */
    private final ReentrantLock waitLock;

    /**
     * Lock condition.
     */
    private final Condition waitingForWorkCondition;

    /**
     * @param threads Number of threads.
     */
    public NodeLock(int threads) {
        this.waitingCounter = 0;
        this.threads = threads;
        this.waitLock = new ReentrantLock();
        this.waitingForWorkCondition = waitLock.newCondition();
    }

    /**
     * Increment waiting counter.
     */
    public void increaseWaitingCounter() {
        waitingCounter++;
    }

    /**
     * Decrement waiting counter.
     */
    public void decreaseWaitingCounter() {
        waitingCounter--;
    }

    /**
     * @return Current waiting counter.
     */
    public int getWaitingCounter() {
        return waitingCounter;
    }

    /**
     * @return Number of threads.
     */
    public int getThreads() {
        return threads;
    }

    /**
     * Decrement number of working threads.
     */
    public void failedThread() {
        threads--;
    }

    /**
     * @return Lock instance.
     */
    public ReentrantLock getWaitLock() {
        return waitLock;
    }

    /**
     * @return Lock condition instance.
     */
    public Condition getWaitingForWorkCondition() {
        return waitingForWorkCondition;
    }
}
