package eu.profinit.manta.graphplayground.model.manta.merger;

import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats;

/**
 * Single result of merging processor.
 *
 * @author tfechtner.
 */
public final class MergerProcessorResult {

    /**
     * Type of result.
     */
    private final ProcessingResult.ResultType resultType;

    /**
     * Type of merged object.
     */
    private final DataflowObjectFormats.ItemTypes itemType;

    /**
     * @param resultType Type of result.
     * @param itemType Type of merged object.
     */
    public MergerProcessorResult(ProcessingResult.ResultType resultType, DataflowObjectFormats.ItemTypes itemType) {
        super();
        this.resultType = resultType;
        this.itemType = itemType;
    }

    /**
     * @return Type of result.
     */
    public ProcessingResult.ResultType getResultType() {
        return resultType;
    }

    /**
     * @return Type of merged object.
     */
    public DataflowObjectFormats.ItemTypes getItemType() {
        return itemType;
    } 
    
    
}
