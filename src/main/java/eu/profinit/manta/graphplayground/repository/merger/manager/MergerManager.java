package eu.profinit.manta.graphplayground.repository.merger.manager;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import eu.profinit.manta.graphplayground.model.manta.merger.MergingGroup;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.MergerOutput;
import org.springframework.stereotype.Service;

/**
 * Merger manager.
 * Ensures that only one type of merging requests (type, revision) is allowed to proceed.
 *
 * @author tpolacok.
 */
@Service
public class MergerManager {

    private final CommonTypeMergerManager commonMergerManager;

    /**
     * Lock to synchronize access.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition on which requests of different type wait.
     */
    private final Condition lockCondition = lock.newCondition();

    /**
     * Group that is currently processed.
     */
    private MergingGroup lockedGroup;

    /**
     * Number of registered processes.
     */
    private int registered = 0;

    /**
     * @param commonMergerManager Processor that handles merging of files and further database merging.
     */
    public MergerManager(CommonTypeMergerManager commonMergerManager) {
        this.commonMergerManager = commonMergerManager;
    }

    /**
     * Registers merging request.
     * If it is of type that is currently processed it is allowed to proceed, else it is put to sleep.
     * @param mergingGroup Merging type of request.
     */
    public void register(MergingGroup mergingGroup) {
        try {
            lock.lock();
            if (lockedGroup == null) {
                init(mergingGroup);
            } else if (mergingGroup.equals(lockedGroup)) {
                registered++;
            } else {
                while (true) {
                    try {
                        lockCondition.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (lockedGroup == null) {
                        init(mergingGroup);
                        return;
                    } else if (lockedGroup.equals(mergingGroup)) {
                        registered++;
                        return;
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Initializes merging group for processor and lock.
     * @param mergingGroup Merging group.
     */
    private void init(MergingGroup mergingGroup) {
        registered = 1;
        lockedGroup = mergingGroup;
        commonMergerManager.setMergingGroup(mergingGroup);
    }

    /**
     * Request has finished.
     * If the counter is 0 waiting threads are signaled.
     */
    public void unregister() {
        try {
            lock.lock();
            registered--;
            if (registered == 0) {
                lockedGroup = null;
                lockCondition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Merge input using processor.
     * @param mergedObjectsGroups List of all merged groups containing merge objects.
     * @return Merger output.
     */
    public MergerOutput merge(List<List<List<String>>> mergedObjectsGroups) {
        return commonMergerManager.submit(mergedObjectsGroups);
    }
}
