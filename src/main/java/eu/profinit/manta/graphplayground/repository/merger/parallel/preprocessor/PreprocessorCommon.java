package eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import eu.profinit.manta.graphplayground.model.manta.merger.parallel.container.MergeContainer;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.EdgeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.RawMergeQuery;
import eu.profinit.manta.graphplayground.repository.stored.merger.StoredMerger;

/**
 * Common methods for preprocessing.
 *
 * @author tpolacok
 */
public class PreprocessorCommon {

    /**
     * Trim largest container.
     * @param edgeMergeContainers Merge objects containers.
     * @param conflictingEdges Conflicting edges.
     */
    public static void trimMax(List<MergeContainer> edgeMergeContainers,
                               List<CommonMergeQuery> conflictingEdges) {
        List<MergeContainer> sorted = edgeMergeContainers
                .stream()
                .sorted((f1, f2) -> Long.compare(f2.getContent().size(), f1.getContent().size()))
                .collect(Collectors.toList());

        int secondSize = sorted.get(1).getContent().size();
        if (sorted.get(0).getContent().size() > secondSize * StoredMerger.TRUNCATE_COEFICIENT) {
            List<CommonMergeQuery> maxMergedObjects = sorted.get(0).getContent();
            conflictingEdges.addAll(maxMergedObjects.subList(secondSize, maxMergedObjects.size()));
            sorted.get(0).truncate(secondSize);
        }
    }

    /**
     * Adds attributes to edge processing.
     * @param edgeMergeContainers Merge objects containers.
     * @param edgeToAttributes Edge to attributes mappings.
     */
    public static void addAttributes(List<MergeContainer> edgeMergeContainers,
                                      Map<String, List<CommonMergeQuery>> edgeToAttributes) {
        for (MergeContainer mergeContainer: edgeMergeContainers) {
            List<CommonMergeQuery> allAttributes = new ArrayList<>();
            mergeContainer.consumeLogic(commonMergeObject
                    -> allAttributes.addAll(edgeToAttributes
                            .getOrDefault(
                                    ((EdgeQuery)commonMergeObject).identify(),
                                    Collections.emptyList()
                            )
            ));
            mergeContainer.addAll(allAttributes);
        }
    }

    /**
     * Splits input list into bucket lists and wraps each list in MergeMergeContainer
     * @param mergeObjects List of merge objects.
     * @param buckets Number of buckets.
     * @return List of MergeContainers containing split objects.
     */
    public static List<MergeContainer> splitListToMergeContainer(List<List<String>> mergeObjects, int buckets) {
        return Lists.partition(mergeObjects, buckets)
                .stream()
                .map(PreprocessorCommon::convertToMergeQueries)
                .map(MergeContainer::new)
                .collect(Collectors.toList());
    }

    /**
     * Converts to merge objects wrappers.
     * @param mergeObjects List of merge objects.
     * @return List of {@link CommonMergeQuery}
     */
    private static List<CommonMergeQuery> convertToMergeQueries(List<List<String>> mergeObjects) {
        return mergeObjects.stream().map(RawMergeQuery::new).collect(Collectors.toList());
    }

    /**
     * Size of initial split for a given group.
     * @param nodeAttributes Group of node attributes.
     * @param groups Number of groups to split to.
     * @return Size of the split.
     */
    public static int getSplitSize(List<List<String>> nodeAttributes, int groups) {
        int splitSize = nodeAttributes.size() / groups + (nodeAttributes.size() % groups);
        splitSize = splitSize == 0 ? 1 : splitSize;
        return splitSize;
    }

}
