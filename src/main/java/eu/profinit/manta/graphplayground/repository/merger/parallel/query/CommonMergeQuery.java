package eu.profinit.manta.graphplayground.repository.merger.parallel.query;

import java.util.List;

import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;

/**
 * Common merge object.
 *
 * Has evaluation method which contains logic for merging given type.
 *
 * @author tpolacok
 */
public interface CommonMergeQuery {

    /**
     * Evaluate the query.
     * @param context Processor context.
     * @return List of merging results or empty list if a merge object evaluated is delayed
     * as it was already added to the final results list.
     */
    List<MergerProcessorResult> evaluate(StoredMergerProcessor mergerProcessor, StoredProcessorContext context);
}
