package eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.MantaRelationshipType;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.EdgeFormat;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.container.MergeContainer;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.phase.MergePhase;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelMergeContainerQueue;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedEdgeQuery;
import eu.profinit.manta.graphplayground.repository.stored.merger.StoredMerger;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Preprocessor for edges and edge attributes.
 * Splitting is done by respecting nodes.
 *
 * @author tpolacok
 */
public class EdgeNodePreprocessor {

    /**
     * Splits edges into buckets.
     * Perspective edges are split separately because their processing might take a lot more time.
     * @param edges List of node attributes.
     * @param buckets Number of splitting containers.
     * @return Merge phase.
     */
    public static MergePhase initialSplit(List<List<String>> edges, int buckets) {
        Map<Boolean, List<List<String>> > groups =
                edges.stream()
                        .collect(Collectors.partitioningBy(
                                mergeObject ->
                                        Objects.equals(
                                                MantaRelationshipType.parseFromGraphType(
                                                        mergeObject.get(EdgeFormat.TYPE.getIndex())
                                                ),
                                                MantaRelationshipType.PERSPECTIVE
                                        )
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
     * Splits delayed edge queries into several buckets along with their attributes.
     * @param edgeQueries List of delayed edge queries.
     * @param buckets Number of different containers.
     * @return List of parallel queues.
     */
    public static List<ParallelMergeContainerQueue> redistributeEdgeQueries(List<CommonMergeQuery> edgeQueries,
                                                                            Map<String, List<CommonMergeQuery>> edgeToAttributes,
                                                                            int buckets) {
        Map<Long, MergeContainer> nodeToContainer =  new HashMap<>();
        List<CommonMergeQuery> conflictingEdges = new ArrayList<>();
        List<MergeContainer> edgeContainers =
                Stream.generate(MergeContainer::new)
                        .limit(buckets)
                        .collect(Collectors.toList());
        for (CommonMergeQuery edge : edgeQueries) {
            DelayedEdgeQuery delayedEdgeQuery = (DelayedEdgeQuery)edge;
            addToContainer(
                    nodeToContainer,
                    delayedEdgeQuery.getSourceNodeId(),
                    delayedEdgeQuery.getTargetNodeId(),
                    delayedEdgeQuery,
                    conflictingEdges,
                    edgeContainers
            );
        }
        PreprocessorCommon.trimMax(edgeContainers, conflictingEdges);
        PreprocessorCommon.addAttributes(edgeContainers, edgeToAttributes);
        return List.of(
                new ParallelMergeContainerQueue(edgeContainers),
                new ParallelMergeContainerQueue(Collections.singletonList(new MergeContainer(conflictingEdges)))
        );
    }

    /**
     * Adds edge to container or to conflicting edges list.

     * @param nodeToContainer Mapping of node id to container
     * @param sourceId Source id of edge
     * @param targetId Target id of edge.
     * @param edge Edge object.
     * @param conflictingEdges List of conflicting edges.
     * @param edgeContainers Buckets of the edges.
     */
    protected static void addToContainer(Map<Long, MergeContainer> nodeToContainer,
                                         Long sourceId,
                                         Long targetId,
                                         DelayedEdgeQuery edge,
                                         List<CommonMergeQuery> conflictingEdges,
                                         List<MergeContainer> edgeContainers) {
        MergeContainer source = nodeToContainer.get(sourceId);
        MergeContainer target = nodeToContainer.get(targetId);
        if (source == null) {
            if (target == null) {
                MergeContainer container = edgeContainers
                        .stream()
                        .min(Comparator.comparingInt(mergeContainer -> mergeContainer.getContent().size()))
                        .orElseThrow(NoSuchElementException::new);

                container.add(edge);
                nodeToContainer.put(sourceId, container);
                nodeToContainer.put(targetId, container);
            } else {
                nodeToContainer.put(sourceId, target);
                target.add(edge);
            }
        } else {
            if (target == null) {
                nodeToContainer.put(targetId, source);
                source.add(edge);
            }
            else {
                if (source == target) source.add(edge);
                else conflictingEdges.add(edge);
            }
        }
    }

    /**
     * Preprocesses edge attributes and delayed edge queries.
     *
     * The cycle inside this method serves to recursively
     * split the remaining edges into containers.
     *
     * @param edgeQueries Delayed edge queries.
     * @param edgeToAttributes Mapping of edge local id to its attributes definitions
     * @param buckets Number of buckets.
     * @return Merge phase.
     */
    public static MergePhase splitDelayedEdges(List<CommonMergeQuery> edgeQueries,
                                               Map<String, List<CommonMergeQuery>> edgeToAttributes,
                                               int buckets) {
        List<ParallelMergeContainerQueue> edgeQueues = new ArrayList<>();
        List<CommonMergeQuery> restOfEdges = split(edgeQueries, edgeToAttributes,
                edgeQueues, buckets);
        int lastSize = restOfEdges.size();
        while (true) {
            restOfEdges = split(restOfEdges, edgeToAttributes, edgeQueues, buckets);
            int newSize = restOfEdges.size();
            if (newSize == lastSize || newSize < StoredMerger.EDGE_REMAINING_SPLIT_THRESHOLD
                    || edgeQueues.get(edgeQueues.size() - 1).getInitialSize() < StoredMerger.EDGE_COUNT_SPLIT_THRESHOLD) break;
            lastSize = newSize;
        }
        edgeQueues.add(new ParallelMergeContainerQueue(
                Collections.singletonList(new MergeContainer(restOfEdges)))
        );
        return new MergePhase(edgeQueues);
    }

    /**
     * Splits list of edges to several containers by buckets parameter and returns the remaining edges.
     *
     * @param edgeQueries List of delayed edge queries
     * @param edgeToAttributes Edge to edge attributes mappings
     * @param edgeQueues List of parallel queues.
     * @param buckets Number of buckets.
     * @return List of remaining edge queries.
     */
    private static List<CommonMergeQuery> split(List<CommonMergeQuery> edgeQueries,
                                                Map<String, List<CommonMergeQuery>> edgeToAttributes,
                                                List<ParallelMergeContainerQueue> edgeQueues,
                                                int buckets) {
        List<ParallelMergeContainerQueue> queues = EdgeNodePreprocessor.redistributeEdgeQueries(edgeQueries,
                edgeToAttributes, buckets);
        edgeQueues.add(queues.get(0));
        MergeContainer edgeContainer = queues.get(1).poll();
        return edgeContainer.getContent();
    }
}
