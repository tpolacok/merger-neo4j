package eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor;

import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.container.MergeContainer;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.phase.MergePhase;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelMergeContainerQueue;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelNodeQueue;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelNodeWithAttributesQueue;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedNodeAttributeQuery;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Preprocessor for node attributes.
 *
 * @author tpolacok
 */
public class NodeAttributePreprocessor {

    /**
     * Splits node attributes into maps_to and classic node attributes.
     * Reason being that maps_to node attributes might take substantially longer processing time.
     * @param nodeAttributes List of node attributes.
     * @param buckets Number of splitting containers.
     * @return Merge phase.
     */
    public static MergePhase initialSplit(List<List<String>> nodeAttributes, int buckets) {
        Map<Boolean, List<List<String>> > groups =
                nodeAttributes.stream()
                        .collect(Collectors.partitioningBy(
                                mergeObject ->
                                        mergeObject.get(DataflowObjectFormats.AttributeFormat.KEY.getIndex())
                                            .equals(StoredMergerProcessor.MAPS_TO)
                        ));
        return new MergePhase(
                List.of(
                    new ParallelMergeContainerQueue(PreprocessorCommon.splitListToMergeContainer(groups.get(true),
                            PreprocessorCommon.getSplitSize(groups.get(true), buckets))),
                    new ParallelMergeContainerQueue(PreprocessorCommon.splitListToMergeContainer(groups.get(false),
                            PreprocessorCommon.getSplitSize(groups.get(false), buckets)))
            )
        );
    }

    /**
     * Splits delayed node attributes queries into several buckets
     * @param nodeAttributeQueries List of delayed node attribute queries.
     * @param buckets Number of different containers.
     * @return Merge phase.
     */
    public static MergePhase redistributeNodeAttributesQueries(
            List<CommonMergeQuery> nodeAttributeQueries, int buckets) {
        nodeAttributeQueries.sort(Comparator.comparingLong(x -> ((DelayedNodeAttributeQuery)x).getNodeId()));
        List<List<DelayedNodeAttributeQuery>> nodeAttributeContainers =
                Stream.generate(ArrayList<DelayedNodeAttributeQuery>::new)
                        .limit(buckets)
                        .collect(Collectors.toList());

        long splitSize = nodeAttributeQueries.size() / buckets;
        int boundary = buckets - 1;
        int currentBucket = 0;
        int currentCount = 0;
        Long lastId = -1L;
        for (CommonMergeQuery attribute: nodeAttributeQueries) {
            DelayedNodeAttributeQuery attributeQuery = (DelayedNodeAttributeQuery) attribute;
            Long nodeId = attributeQuery.getNodeId();
            if (currentCount >= splitSize && !nodeId.equals(lastId) && currentBucket < boundary) {
                currentCount = 0;
                currentBucket++;
            }
            nodeAttributeContainers.get(currentBucket).add(attributeQuery);
            lastId = nodeId;
            currentCount++;
        }
        return new MergePhase(
            List.of(
                new ParallelMergeContainerQueue(
                        nodeAttributeContainers.stream()
                                .map(ArrayList<CommonMergeQuery>::new)
                                .map(MergeContainer::new)
                                .collect(Collectors.toList())
                )
            )
        );
    }

    /**
     * Preprocesses node attributes by storing nodes and their attributes.
     * Nodes are assigned their attributes.
     * MAPS_TO attributes are collected as isolated list.
     *
     * @param nodeAttributes List of merged objects.
     * @return List of {@link ParallelMergeContainerQueue} containing buckets of node attributes to process concurrently.
     */
    public static ParallelNodeWithAttributesQueue preprocessWithNodes(ParallelNodeQueue nodeQueue,
                                                                      List<List<String>> nodeAttributes) {
        List<List<String>> mapToAttributes = new ArrayList<>();
        Map<String, List<List<String>>> mapNodeToAttributes = new HashMap<>();
        for (List<String> attribute: nodeAttributes) {
            String key = attribute.get(DataflowObjectFormats.AttributeFormat.KEY.getIndex());
            if (StoredMergerProcessor.MAPS_TO.equals(key)) {
                mapToAttributes.add(attribute);
            } else {
                mapNodeToAttributes.computeIfAbsent(attribute.get(1), k -> new ArrayList<>()).add(attribute);
            }
        }
        return new ParallelNodeWithAttributesQueue(nodeQueue, mapNodeToAttributes, mapToAttributes);
    }


}
