package eu.profinit.manta.graphplayground.model.manta.merger.stored;

import java.util.Map;
import java.util.stream.Collectors;

import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult;
import eu.profinit.manta.graphplayground.model.manta.merger.SourceCodeMapping;

/**
 * Output object for stored procedure.
 *
 * @author tpolacok
 */
public class MergerOutput {
    /**
     * Total time it took for the merge.
     */
    public Long time;

    /**
     * Newly added objects.
     */
    public Map<String, Integer> newObjects;

    /**
     * Error objects.
     */
    public Map<String, Integer> errorObjects;

    /**
     * Existed objects.
     */
    public Map<String, Integer> existedObjects;

    /**
     * Unknown types.
     */
    public Long unknownTypes;

    /**
     * Total number of processed objects.
     */
    public Long processedObjects;

    /**
     * Requested source codes.
     */
    public Map<String, String> requestedSourceCodes;

    /**
     * Constructor for output object.
     * @param time Total merge time.
     * @param newObjects New object count.
     * @param errorObjects Error object count.
     * @param existedObjects Existed object count.
     * @param unknownTypes Unknown object count.
     * @param processedObjects Total processed object count.
     * @param requestedSourceCodes Requested source codes.
     */
    public MergerOutput(Long time, Map<String, Integer> newObjects, Map<String, Integer> errorObjects,
                        Map<String, Integer>existedObjects, Long unknownTypes, Long processedObjects,
                        Map<String, String> requestedSourceCodes) {
        this.time = time;
        this.newObjects = newObjects;
        this.errorObjects = errorObjects;
        this.existedObjects = existedObjects;
        this.unknownTypes = unknownTypes;
        this.processedObjects = processedObjects;
        this.requestedSourceCodes = requestedSourceCodes;
    }

    /**
     * @param time Total merge time.
     * @param processingResult Merge Processing result.
     */
    public MergerOutput(Long time, ProcessingResult processingResult) {
        this.time = time;
        this.newObjects = processingResult.getNewObjects();
        this.errorObjects = processingResult.getErrorObjects();
        this.existedObjects = processingResult.getExistedObjects();
        this.unknownTypes = (long) processingResult.getUnknownTypes();
        this.processedObjects = (long) processingResult.getProcessedObjects();
        this.requestedSourceCodes = processingResult.getRequestedSourceCodes().stream()
                .collect(Collectors.toMap(SourceCodeMapping::getLocalId, SourceCodeMapping::getDbId));
    }

    /**
     * @return Total time it took for the merge.
     */
    public Long getTime() {
        return time;
    }

    /**
     * @return Count of new objects.
     */
    public Map<String, Integer> getNewObjects() {
        return newObjects;
    }

    /**
     * @return Count of error objects.
     */
    public Map<String, Integer> getErrorObjects() {
        return errorObjects;
    }

    /**
     * @return Count of existed objects.
     */
    public Map<String, Integer> getExistedObjects() {
        return existedObjects;
    }

    /**
     * @return Count of unknown type objects.
     */
    public Long getUnknownTypes() {
        return unknownTypes;
    }

    /**
     * @return Count of processed object.
     */
    public Long getProcessedObjects() {
        return processedObjects;
    }

    /**
     * @return Requested source code mappings.
     */
    public Map<String, String> getRequestedSourceCodes() {
        return requestedSourceCodes;
    }

    @Override
    public String toString() {
        return "MergerOutput{" +
                "time=" + time +
                ", newObjects=" + newObjects +
                ", errorObjects=" + errorObjects +
                ", existedObjects=" + existedObjects +
                ", unknownTypes=" + unknownTypes +
                ", processedObjects=" + processedObjects +
                ", requestedSourceCodes=" + requestedSourceCodes +
                '}';
    }
}