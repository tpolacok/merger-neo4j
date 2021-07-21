package eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.input;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import eu.profinit.manta.graphplayground.model.manta.merger.stored.MergerOutput;

/**
 * Context for file merging.
 * Contains lock and condition for when multiple merge requests are waiting for shared merging result.
 *
 * @author tpolacok
 */
public class FileMergingContext {

    /**
     * Lock for condition.
     */
    private ReentrantLock sharedFileMergeLock = new ReentrantLock();

    /**
     * File merging condition.
     */
    private Condition sharedFileMerging = sharedFileMergeLock.newCondition();

    /**
     * Shared output for all participating requests.
     */
    private MergerOutput sharedOutput;

    /**
     * Total file-merge subscribed processes.
     */
    private int fileMergingSubscribers = 1;

    /**
     * Merged file.
     */
    private MergedFile mergedFile = new MergedFile();

    /**
     * New merge request.
     */
    public void subscribe() {
        fileMergingSubscribers++;
    }

    /**
     * Merge request done.
     */
    public void unsubscribe() {
        fileMergingSubscribers--;
    }

    /**
     * @return Flag whether all merge requests have been merged.
     */
    public boolean allFilesMerged() {
        return fileMergingSubscribers == 0;
    }

    /**
     * @return Merged file.
     */
    public MergedFile getMergedFile() {
        return mergedFile;
    }

    /**
     * Put thread to sleep until shared merge is done.
     * @return Merger output.
     */
    public MergerOutput waitForResult() {
        MergerOutput output;
        sharedFileMergeLock.lock();
        while (true) {
            try {
                sharedFileMerging.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (sharedOutput != null) {
                output = sharedOutput;
                break;
            }
        }
        sharedFileMergeLock.unlock();
        return output;
    }

    /**
     * Signal waiting threads that merger work is done.
     * @param output Merger output.
     */
    public void doneResult(MergerOutput output) {
        try {
            sharedFileMergeLock.lock();
            sharedOutput = output;
            sharedFileMerging.signalAll();
        } finally {
            sharedFileMergeLock.unlock();
        }
    }
}
