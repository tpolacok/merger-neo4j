package eu.profinit.manta.graphplayground.repository.merger.parallel.query;

import java.util.List;

import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.EdgeFormat;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;

/**
 * Raw merge query representing edge.
 *
 * @author tpolacok
 */
public class EdgeRawMergeQuery extends RawMergeQuery implements EdgeQuery {

    /**
     * @param content Object content.
     */
    public EdgeRawMergeQuery(List<String> content) {
        super(content);
    }

    @Override
    public List<MergerProcessorResult> evaluate(StoredMergerProcessor mergerProcessor,
                                                StoredProcessorContext context) {
        List<MergerProcessorResult> result = mergerProcessor.mergeObject(content.toArray(new String[0]), context);
        if (result.size() == 1 && result.get(0).getResultType() == ProcessingResult.ResultType.ALREADY_EXIST) {
            //evaluate attributes
            context.getEdgeAttributesForEdge(content.get(
                    EdgeFormat.ID.getIndex()
            )).forEach(edgeAttribute -> {
                List<MergerProcessorResult> edgeAttributeResults = edgeAttribute.evaluate(mergerProcessor, context);
                result.addAll(edgeAttributeResults);
            });
        }
        return result;
    }

    @Override
    public String identify() {
        return content.get(EdgeFormat.ID.getIndex());
    }
}