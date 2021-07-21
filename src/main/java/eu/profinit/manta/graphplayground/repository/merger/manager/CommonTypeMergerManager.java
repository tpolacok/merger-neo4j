package eu.profinit.manta.graphplayground.repository.merger.manager;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import eu.profinit.manta.graphplayground.model.manta.merger.MergingGroup;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.MergerOutput;
import eu.profinit.manta.graphplayground.repository.merger.connector.MergerRepository;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.input.FileMergerProcessor;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.input.FileMergingContext;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.input.InputFileContext;
import org.springframework.stereotype.Repository;

/**
 * Manager handles merging requests submission to the database service.
 * In case of request being handled other requests are merged together and then submitted to the database.
 *
 * @author tpolacok
 */
@Repository
public class CommonTypeMergerManager {

    /**
     * File merger processor.
     */
    private final FileMergerProcessor fileMergerProcessor = new FileMergerProcessor();

    /**
     * File database merge lock.
     */
    private final ReentrantLock databaseMergeLock = new ReentrantLock();

    /**
     * Waiting condition.
     */
    private final Condition mergingCondition = databaseMergeLock.newCondition();

    /**
     * Database merge flag.
     */
    private boolean isDatabaseMerging = false;

    /**
     * Wait flag.
     */
    private boolean isRequestWaiting = false;

    /**
     * File merge flag.
     */
    private boolean areRequestsMerging = false;

    /**
     * Context for merged files.
     */
    private FileMergingContext currentFileMergingContext;

    /**
     * Merged group.
     */
    private MergingGroup mergingGroup;

    /**
     * Merger repository for database access.
     */
    private final MergerRepository mergerRepository;

    /**
     * @param mergerRepository Merger repository for database access.
     */
    public CommonTypeMergerManager(MergerRepository mergerRepository) {
        this.mergerRepository = mergerRepository;
    }

    /**
     * Submits list of merge groups for processing.
     *
     * If fileMerging is true -> request is subscribed and merged.
     *
     * If both fileMerging and databaseMerging flags are true -> request is further processed with repository.
     *
     * If fileMerging is false and databaseMerging is true -> If waiting is true,
     * file merging is intialized and sleeping threads are woken, else thread is put to sleep.
     *
     * @param mergedObjectsGroups List of merge groups for processing.
     * @return Merger output.
     */
    public MergerOutput submit(List<List<List<String>>> mergedObjectsGroups) {
        databaseMergeLock.lock();
        if (areRequestsMerging) {
            return subscribeRequestAndMerge(mergedObjectsGroups);
        }
        if (isDatabaseMerging) {
            if (isRequestWaiting) {
                isRequestWaiting = false;
                areRequestsMerging = true;
                currentFileMergingContext = new FileMergingContext();
                mergingCondition.signalAll();
                databaseMergeLock.unlock();
                return mergeMultipleFiles(mergedObjectsGroups);
            } else {
                isRequestWaiting = true;
                while (true) {
                    try {
                        mergingCondition.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (areRequestsMerging) {
                        return subscribeRequestAndMerge(mergedObjectsGroups);
                    } else if (!isDatabaseMerging) {
                        isRequestWaiting = false;
                        isDatabaseMerging = true;
                        databaseMergeLock.unlock();
                        return databaseMergeFromSingle(mergedObjectsGroups);
                    }
                }
            }
        } else {
            isDatabaseMerging = true;
            databaseMergeLock.unlock();
            return databaseMergeFromSingle(mergedObjectsGroups);
        }
    }

    /**
     * Subscribes request to current merging context and unlocks the lock.
     * @param mergedObjectsGroups List of merge objects groups.
     * @return Merger output.
     */
    private MergerOutput subscribeRequestAndMerge(List<List<List<String>>> mergedObjectsGroups) {
        currentFileMergingContext.subscribe();
        databaseMergeLock.unlock();
        return mergeMultipleFiles(mergedObjectsGroups);
    }

    /**
     * Merge multiple files.
     * File from request is merged into shared context.
     * If all subscribed files are merged and databaseMerging flag is false -> merged context
     * is further processed with repository, else request is put to sleep and waits for result.
     * @param mergedObjectsGroups  List of merge groups for processing.
     * @return Merger output.
     */
    private MergerOutput mergeMultipleFiles(List<List<List<String>>> mergedObjectsGroups) {
        InputFileContext fileContext = new InputFileContext();
        for (List<List<String>> mergeGroup : mergedObjectsGroups) {
            for (List<String> mergeObject : mergeGroup) {
                fileMergerProcessor.mergeObject(mergeObject, currentFileMergingContext.getMergedFile(), fileContext);
            }
        }
        databaseMergeLock.lock();
        currentFileMergingContext.unsubscribe();
        MergerOutput mergerOutput;
        FileMergingContext context = currentFileMergingContext;
        if (context.allFilesMerged() && !isDatabaseMerging) {
            areRequestsMerging = false;
            isDatabaseMerging = true;
            databaseMergeLock.unlock();
            mergerOutput = databaseMergeFromMultiple(context);
        } else {
            databaseMergeLock.unlock();
            mergerOutput = context.waitForResult();
        }
        return mergerOutput;
    }

    /**
     * Merged context is sent to merger repository.
     * Upon receiving result, other participating requests are woken and receive response.
     * Following, it is checked whether there is another merged context waiting for database processing
     * and is further processed if that is the case.
     * @param context Context for file merging.
     * @return Merger output.
     */
    private MergerOutput databaseMergeFromMultiple(FileMergingContext context) {
        MergerOutput mergerOutput = mergeToDatabase(context.getMergedFile().getMergeObjects());
        context.doneResult(mergerOutput);
        finishDatabaseMerging();
        return mergerOutput;
    }

    /**
     * Single file request is sent to merger repository.
     * Following, it is checked whether there is another merged context waiting for database processing
     * and is further processed if that is the case
     *
     * @param mergedObjectsGroups List of merge groups for processing.
     * @return Merger output.
     */
    private MergerOutput databaseMergeFromSingle(List<List<List<String>>> mergedObjectsGroups) {
        MergerOutput mergerOutput = mergeToDatabase(mergedObjectsGroups);
        finishDatabaseMerging();
        return mergerOutput;
    }

    /**
     * Checks whether there is another merged context waiting for database processing
     * and is further processed if that is the case.
     *
     * The move of unlocking after if branching is not possible because the last called method in the first branch
     * submits the merged requests to the database - but in the meanwhile the lock
     * is not held which allows for another merge requests to be merged and then
     * recursive behavior is invoked in the database submitting part.
     *
     * Sets databaseMerging flag to false.
     */
    private void finishDatabaseMerging() {
        databaseMergeLock.lock();
        FileMergingContext context = currentFileMergingContext;
        if (areRequestsMerging && context.allFilesMerged()) {
            areRequestsMerging = false;
            databaseMergeLock.unlock();
            databaseMergeFromMultiple(context);
        } else {
            isDatabaseMerging = false;
            mergingCondition.signalAll();
            databaseMergeLock.unlock();
        }
    }

    /**
     * Submits files to merge to the merge repository.
     * @param mergedObjectsGroups List of merge groups for processing.
     * @return Merger output.
     */
    private MergerOutput mergeToDatabase(List<List<List<String>>> mergedObjectsGroups) {
        return mergerRepository.merge(mergingGroup, mergedObjectsGroups);
    }

    /**
     * Sets current merging group.
     * @param mergingGroup Merging group.
     */
    public void setMergingGroup(MergingGroup mergingGroup) {
        this.mergingGroup = mergingGroup;
    }
}
