package eu.profinit.manta.graphplayground.repository.merger.processor;

import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;

import java.util.List;

/**
 * Processor responsible for merging single objects from the input file to the metadata repository.
 *
 * @author tfechtner
 * @author tpolacok
 */
public interface MergerProcessor<ProcessorContext> {

    /**
     * Merges object based on its type.
     * @param itemParameters Parameters of the merged object.
     * @param context Local to database context.
     * @return List of merging results.
     */
    List<MergerProcessorResult> mergeObject(String[] itemParameters, ProcessorContext context);
}
