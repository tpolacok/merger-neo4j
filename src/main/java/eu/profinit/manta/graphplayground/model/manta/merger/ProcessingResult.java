package eu.profinit.manta.graphplayground.model.manta.merger;

import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.ItemTypes;

import java.util.*;

/**
 * Class representing merging result.
 *
 * @author tfechtner.
 */
public class ProcessingResult {

    /**
     * Type of merging result.
     */
    public enum ResultType {
        /**
         * New object created.
         */
        NEW_OBJECT,

        /**
         * Object existed - its revision updated.
         */
        ALREADY_EXIST,

        /**
         * Problem with merging occurred.
         */
        ERROR;
    }
    
    /**
     * Number of new objects indexed by {@link ItemTypes}.
     */
    private int[] newObjects = new int[ItemTypes.values().length];

    /**
     * Number of error objects indexed by {@link ItemTypes}.
     */
    private int[] errorObjects = new int[ItemTypes.values().length];

    /**
     * Number of already existing objects indexed by {@link ItemTypes}.
     */
    private int[] existedObjects = new int[ItemTypes.values().length];

    /**
     * Number of unknown types processed.
     */
    private int unknownTypes;

    /**
     * {@link ResultType} of last processed object.
     */
    private ResultType lastResultType;

    /**
     * Total number of rollbacks.
     */
    private int rollbackNumber;

    /**
     * Length of longest rollback chain during one attempt.
     */
    private int longestRollbackChain;

    /**
     * Total number of processed objects.
     */
    private int processedObjects;

    /**
     * Number of new objects indexed by {@link ItemTypes}.
     */
    private Set<SourceCodeMapping> requestedSourceCodes = new HashSet<>();

    /**
     * Increases number of unknown processed objects.
     */
    public void increaseUnknownObject() {
        processedObjects++;
        unknownTypes++;
    }

    /**
     * Saves result of processed object.
     * @param processResource Result of processing.
     * @param itemType Type of object.
     */
    public void saveResult(ResultType processResource, ItemTypes itemType) {
        int index = itemType.ordinal();
        lastResultType = processResource;
        processedObjects++;
        switch (processResource) {
        case ERROR:
            errorObjects[index]++;
            break;
        case NEW_OBJECT:
            newObjects[index]++;
            break;
        case ALREADY_EXIST:
            existedObjects[index]++;
            break;
        default:
            throw new IllegalArgumentException("Unknown type of result type.");
        }
    }

    /**
     * Saves number of rollback from an attempt.
     * @param newRollbacks Number of rollback from an attempt.
     */
    public void saveRollbacks(int newRollbacks) {
        rollbackNumber += newRollbacks;
        if (longestRollbackChain < newRollbacks) {
            longestRollbackChain = newRollbacks;
        }
    }

    /**
     * Adds content of other {@link ProcessingResult} object.
     * @param otherResult Other merging result.
     */
    public void add(ProcessingResult otherResult) {
        unknownTypes += otherResult.getUnknownTypes();
        processedObjects += otherResult.getProcessedObjects();
        lastResultType = otherResult.getLastResultType();
        rollbackNumber += otherResult.getRollbackNumber();

        if (this.longestRollbackChain < otherResult.getLongestRollbackChain()) {
            longestRollbackChain = otherResult.getLongestRollbackChain();
        }

        for (ItemTypes type : ItemTypes.values()) {
            newObjects[type.ordinal()] += otherResult.newObjects[type.ordinal()];
            errorObjects[type.ordinal()] += otherResult.errorObjects[type.ordinal()];
            existedObjects[type.ordinal()] += otherResult.existedObjects[type.ordinal()];
        }

        requestedSourceCodes.addAll(otherResult.requestedSourceCodes);

    }

    /**
     * Transforms array of object numbers to map indexed by {@link ItemTypes}.
     * @param array Array to transform.
     * @return Transformed map.
     */
    private Map<String, Integer> transformArrayToMap(int[] array) {
        Map<String, Integer> resultMap = new HashMap<String, Integer>();
        for (ItemTypes itemType : ItemTypes.values()) {
            resultMap.put(itemType.getText(), array[itemType.ordinal()]);
        }
        return resultMap;
    }

    /**
     * @return Total number of rollbacks.
     */
    public int getRollbackNumber() {
        return rollbackNumber;
    }

    /**
     * @return Longest rollback chain.
     */
    public int getLongestRollbackChain() {
        return longestRollbackChain;
    }

    /**
     * @return Total number of merged objects.
     */
    public int getProcessedObjects() {
        return processedObjects;
    }

    /**
     * @return Mapping of new merged objects of each type {@link ItemTypes}.
     */
    public Map<String, Integer> getNewObjects() {
        return transformArrayToMap(newObjects);
    }

    /**
     * @return Mapping of error merged objects of each type {@link ItemTypes}.
     */
    public Map<String, Integer> getErrorObjects() {
        return transformArrayToMap(errorObjects);
    }

    /**
     * @return Mapping of existed merged objects of each type {@link ItemTypes}.
     */
    public Map<String, Integer> getExistedObjects() {
        return transformArrayToMap(existedObjects);
    }

    /**
     * @return Total number of unknown types merged.
     */
    public int getUnknownTypes() {
        return unknownTypes;
    }

    /**
     * @return Number of all new objects.
     */
    public int getNewObjectsCount() {
        int total = 0;
        for (int count : newObjects) {
            total += count;
        }
        return total;
    }

    /**
     * @return Type of last added result.
     */
    public ResultType getLastResultType() {
        return lastResultType;
    }

    /**
     * @return Set of requested source codes.
     */
    public Set<SourceCodeMapping> getRequestedSourceCodes() {
        return requestedSourceCodes;
    }

    /**
     * @param requestedSourceCodes Set of requested source codes.
     */
    public void setRequestedSourceCodes(Set<SourceCodeMapping> requestedSourceCodes) {
        this.requestedSourceCodes = requestedSourceCodes;
    }

    @Override
    public String toString() {
        return "ProcessingResult [newObjects=" + Arrays.toString(newObjects) + ", errorObjects="
                + Arrays.toString(errorObjects) + ", existedObjects=" + Arrays.toString(existedObjects)
                + ", unknownTypes=" + unknownTypes + ", lastResultType=" + lastResultType + "]";
    }
}