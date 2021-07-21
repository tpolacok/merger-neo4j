package eu.profinit.manta.graphplayground.model.manta.merger.parallel.container;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;

/**
 * Container containing objects which can be merged within single thread.
 *
 * @author tpolacok
 */
public class MergeContainer {

    /**
     * List of objects to merge.
     */
    private List<CommonMergeQuery> mergeObjects;

    /**
     * @param mergeObjects List of objects to merge.
     */
    public MergeContainer(List<CommonMergeQuery> mergeObjects) {
        this.mergeObjects = mergeObjects;
    }

    /**
     * Initializes empty container.
     */
    public MergeContainer() {
        this.mergeObjects = new ArrayList<>();
    }

    /**
     * Apply logic for each object.
     * @param consumer Consumer
     */
    public void consumeLogic(Consumer<CommonMergeQuery> consumer) {
        mergeObjects.forEach(consumer);
    }

    /**
     * @return Content of container.
     */
    public List<CommonMergeQuery> getContent() {
        return mergeObjects;
    }

    /**
     * @param additionalObject Adds object to container.
     */
    public <T extends CommonMergeQuery> void add(T additionalObject) {
        mergeObjects.add(additionalObject);
    }

    /**
     * @param additionalObjects Adds list of objects to container.
     */
    public <T extends CommonMergeQuery> void addAll(List<T> additionalObjects) {
        mergeObjects.addAll(additionalObjects);
    }

    /**
     * Truncate array size.
     * @param newSize New size of array
     */
    public void truncate(int newSize) {
        mergeObjects = mergeObjects.subList(0, newSize);
    }
}
