package eu.profinit.manta.graphplayground.repository.merger.parallel.query;

import java.util.List;

import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;

/**
 * Default merge object represented by list of strings.
 */
public class RawMergeQuery implements CommonMergeQuery {

    /**
     * Object content.
     */
    protected final List<String> content;

    /**
     * @param content Object content.
     */
    public RawMergeQuery(List<String> content) {
        this.content = content;
    }

    @Override
    public List<MergerProcessorResult> evaluate(StoredMergerProcessor mergerProcessor,
                                                StoredProcessorContext context) {
        return mergerProcessor.mergeObject(content.toArray(new String[0]), context);
    }

    /**
     * @return Object content.
     */
    public List<String> getContent() {
        return content;
    }
}
