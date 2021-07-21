package eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.AttributeFormat;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.RawMergeQuery;

/**
 * Preprocessor for edge attributes.
 *
 * @author tpolacok
 */
public class EdgeAttributePreprocessor {

    /**
     * Creates mapping of edge id to its attributes.
     * @param mergeInput Edge attributes list.
     * @return Created mapping of edge id to its attributes.
     */
    public static Map<String, List<CommonMergeQuery>> preprocessForEdges(List<List<String>> mergeInput) {
        Map<String, List<CommonMergeQuery>> mapEdgeToAttributes = new HashMap<>();
        for (List<String> edgeAttribute: mergeInput) {
            mapEdgeToAttributes.computeIfAbsent(edgeAttribute.get(
                    AttributeFormat.OBJECT_ID.getIndex()), edgeId -> new ArrayList<>())
                    .add(new RawMergeQuery(edgeAttribute));
        }
        return mapEdgeToAttributes;
    }
}

