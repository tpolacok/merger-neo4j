package eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.profinit.manta.graphplayground.model.manta.merger.parallel.container.MergeContainer;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.queue.ParallelMergeContainerQueue;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.RawMergeQuery;
import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats;

/**
 * Preprocessor for edges and edge attributes.
 * Splitting is done by respecting nodes neighborhoods.
 *
 * @author tpolacok
 */
public class EdgeNeighborhoodPreprocessor {

    /**
     * Preprocesses input file by splitting edges to non-conflicting buckets while respecting nodes neighborhoods.
     * Perspective edges are isolated and returned as last element of the list depending on the type.
     * Rest of edges is returned as last element of the list if there are no perspective edges.
     *
     * @param mergeInput List of edges.
     * @param edgeToAttributes Mapping of edge to its attributes.
     * @param buckets Number of bucket to split.
     * @param isolatePerspective flag whether perspective edges should be returned as another list element.
     * @return List of synchronized queues for processing.
     */
    public static List<ParallelMergeContainerQueue> preprocess(List<CommonMergeQuery> mergeInput,
                                                               Map<String, Set<String>> nodeToAdjacentNodes,
                                                               Map<String, List<CommonMergeQuery>> edgeToAttributes,
                                                               int buckets,
                                                               boolean isolatePerspective) {
        Map<String, MergeContainer> nodeToContainer =  new HashMap<>();
        Map<String, MergeContainer> adjacentNodeToContainer = new HashMap<>();
        Set<String> borderingNodes = new HashSet<>();
        List<CommonMergeQuery> conflictingEdges = new ArrayList<>();
        List<CommonMergeQuery> perspectiveEdges = new ArrayList<>();
        List<MergeContainer> edgeContainers =
                Stream.generate(MergeContainer::new)
                        .limit(buckets)
                        .collect(Collectors.toList());
        for (CommonMergeQuery edge: mergeInput) {
            RawMergeQuery edgeTyped = (RawMergeQuery) edge;
            if (isolatePerspective &&
                    DatabaseStructure.MantaRelationshipType.parseFromGraphType(edgeTyped.getContent().get(DataflowObjectFormats.EdgeFormat.TYPE.getIndex()))
                            == DatabaseStructure.MantaRelationshipType.PERSPECTIVE) {
                perspectiveEdges.add(edgeTyped);
            } else {
                processNext(nodeToContainer, adjacentNodeToContainer, borderingNodes,
                        edgeTyped.getContent().get(DataflowObjectFormats.EdgeFormat.SOURCE.getIndex()), edgeTyped.getContent().get(DataflowObjectFormats.EdgeFormat.TARGET.getIndex()), edgeTyped,
                        conflictingEdges, edgeContainers, nodeToAdjacentNodes);
            }
        }
        PreprocessorCommon.trimMax(edgeContainers, conflictingEdges);
        finalize(edgeContainers, nodeToAdjacentNodes);
        PreprocessorCommon.addAttributes(edgeContainers, edgeToAttributes);
        if (isolatePerspective) {
            return List.of(
                    new ParallelMergeContainerQueue(edgeContainers),
                    new ParallelMergeContainerQueue(Collections.singletonList(new MergeContainer(conflictingEdges))),
                    new ParallelMergeContainerQueue(Collections.singletonList(new MergeContainer(perspectiveEdges)))
            );
        } else {
            return List.of(
                    new ParallelMergeContainerQueue(edgeContainers),
                    new ParallelMergeContainerQueue(Collections.singletonList(new MergeContainer(conflictingEdges)))
            );
        }
    }

    /**
     * Adds new edges for following processing/
     * @param edgeContainers Edge containers.
     * @param nodeToAdjacentNodes Map of node to adjacent nodes.
     */
    private static void finalize(List<MergeContainer> edgeContainers,
                                 Map<String, Set<String>> nodeToAdjacentNodes) {
        edgeContainers.forEach(edgeContainer -> edgeContainer.getContent().forEach(edge -> {
            RawMergeQuery edgeTyped = (RawMergeQuery)edge;
            nodeToAdjacentNodes.computeIfAbsent(edgeTyped.getContent().get(DataflowObjectFormats.EdgeFormat.SOURCE.getIndex()), key -> new HashSet<>())
                    .add(edgeTyped.getContent().get(DataflowObjectFormats.EdgeFormat.TARGET.getIndex()));
            nodeToAdjacentNodes.computeIfAbsent(edgeTyped.getContent().get(DataflowObjectFormats.EdgeFormat.TARGET.getIndex()), key -> new HashSet<>())
                    .add(edgeTyped.getContent().get(DataflowObjectFormats.EdgeFormat.SOURCE.getIndex()));
        }));
    }

    /**
     * Adds edge to container or to conflicting nodes list.
     * If both nodes are unassigned they are assigned to smallest container.
     * If either source or target node is bordering - edge is added as conflicting
     * If source and target belong to different containers - edge is conflicting
     * If source and target belong to same containers (or adjacent-belong) nodes and edge are assigned to given container.
     * If node is assigned to container, all its neighboring nodes are adjacent-assigned to given container.
     *
     * Splitting edges into containers not only by the source and target nodes but also
     * by their surrounding nodes to prevent locking issues.
     *
     * @param nodeToContainer Mapping of node id to container
     * @param sourceId Source id of edge
     * @param targetId Target id of edge.
     * @param edge Edge object.
     * @param conflictingEdges List of conflicting edges.
     * @param edgeContainers Buckets of the edges.
     */
    protected static void processNext(Map<String, MergeContainer> nodeToContainer,
                                         Map<String, MergeContainer> adjacentNodeToContainer,
                                         Set<String> borderingNodes,
                                         String sourceId,
                                         String targetId,
                                         RawMergeQuery edge,
                                         List<CommonMergeQuery> conflictingEdges,
                                         List<MergeContainer> edgeContainers,
                                         Map<String, Set<String>> nodeToAdjacentNodes) {
        if (borderingNodes.contains(sourceId) || borderingNodes.contains(targetId)) {
            conflictingEdges.add(edge);
            return;
        }
        MergeContainer source = nodeToContainer.get(sourceId);
        MergeContainer target = nodeToContainer.get(targetId);
        if (source == null) {
            if (target == null) {
                MergeContainer adjacentSource = adjacentNodeToContainer.get(sourceId);
                MergeContainer adjacentTarget = adjacentNodeToContainer.get(targetId);
                if (adjacentSource == null) {
                    if (adjacentTarget == null) {
                        MergeContainer container = edgeContainers
                                .stream()
                                .min(Comparator.comparingInt(a -> a.getContent().size()))
                                .orElseThrow(NoSuchElementException::new);
                        addEdgeToContainerFull(nodeToContainer, adjacentNodeToContainer, nodeToAdjacentNodes,
                                borderingNodes, container, sourceId, targetId, edge);
                    } else {
                        addEdgeToContainerFull(nodeToContainer, adjacentNodeToContainer, nodeToAdjacentNodes,
                                borderingNodes, adjacentTarget, sourceId, targetId, edge);
                    }
                } else {
                    if (adjacentTarget == null) {
                        addEdgeToContainerFull(nodeToContainer, adjacentNodeToContainer, nodeToAdjacentNodes,
                                borderingNodes, adjacentSource, sourceId, targetId, edge);
                    } else {
                        if (adjacentSource != adjacentTarget) {
                            conflictingEdges.add(edge);
                        } else {
                            addEdgeToContainerFull(nodeToContainer, adjacentNodeToContainer, nodeToAdjacentNodes,
                                    borderingNodes, adjacentSource, sourceId, targetId, edge);
                        }
                    }
                }
            } else {
                if (adjacentNodeToContainer.get(sourceId) != null
                    && adjacentNodeToContainer.get(sourceId) != target) {
                    conflictingEdges.add(edge);
                } else {
                    target.add(edge);
                    addToContainer(nodeToContainer, adjacentNodeToContainer, nodeToAdjacentNodes,
                            borderingNodes, target, sourceId);
                }
            }
        } else {
            if (target == null) {
                if (adjacentNodeToContainer.get(targetId) != null
                    && adjacentNodeToContainer.get(targetId) != source) {
                    conflictingEdges.add(edge);
                } else {
                    source.add(edge);
                    addToContainer(nodeToContainer, adjacentNodeToContainer, nodeToAdjacentNodes,
                            borderingNodes, source, targetId);
                }
            } else {
                if (source != target) {
                    conflictingEdges.add(edge);
                } else {
                    source.add(edge);
                }
            }
        }
    }

    private static void addEdgeToContainerFull(Map<String, MergeContainer> nodeToContainer,
                                    Map<String, MergeContainer> adjacentNodeToContainer,
                                    Map<String, Set<String>> nodeToAdjacentNodes,
                                    Set<String> borderingNodes,
                                    MergeContainer container,
                                    String nodeA,
                                    String nodeB,
                                    RawMergeQuery edge) {
        container.add(edge);
        addToContainer(nodeToContainer, adjacentNodeToContainer, nodeToAdjacentNodes,
                borderingNodes, container, nodeA);
        addToContainer(nodeToContainer, adjacentNodeToContainer, nodeToAdjacentNodes,
                borderingNodes, container, nodeB);
    }

    /**
     * Adds node and all its adjacent nodes to given container.
     * @param nodeToContainer Mapping of node to container.
     * @param adjacentNodeToContainer Mapping of adjacent nodes to container.
     * @param nodeToAdjacentNodes Mapping of node to its adjacent nodes.
     * @param borderingNodes Bordering nodes.
     * @param container Container to which adjacent nodes are assigned.
     * @param nodeId Id of the node.
     */
    private static void addToContainer(Map<String, MergeContainer> nodeToContainer,
                                       Map<String, MergeContainer> adjacentNodeToContainer,
                                       Map<String, Set<String>> nodeToAdjacentNodes,
                                       Set<String> borderingNodes,
                                       MergeContainer container,
                                       String nodeId) {
        nodeToContainer.put(nodeId, container);
        addAdjacent(nodeToContainer, adjacentNodeToContainer, nodeToAdjacentNodes, borderingNodes, container, nodeId);
        adjacentNodeToContainer.remove(nodeId);
    }

    /**
     * Assigns all adjacent nodes to given container.
     * If adjacent node belongs to other container it is moved to bordering nodes.
     * @param nodeToContainer Mapping of node to container.
     * @param adjacentNodeToContainer Mapping of adjacent nodes to container.
     * @param nodeToAdjacentNodes Mapping of node to its adjacent nodes.
     * @param borderingNodes Bordering nodes.
     * @param container Container to which adjacent nodes are assigned.
     * @param nodeId Id of the node.
     */
    private static void addAdjacent(Map<String, MergeContainer> nodeToContainer,
                                    Map<String, MergeContainer> adjacentNodeToContainer,
                                    Map<String, Set<String>> nodeToAdjacentNodes,
                                    Set<String> borderingNodes,
                                    MergeContainer container,
                                    String nodeId) {
        for (String adjacent: nodeToAdjacentNodes.getOrDefault(nodeId, Collections.emptySet())) {
            MergeContainer nodeContainer = nodeToContainer.get(adjacent);
            if (nodeContainer != null) continue;
            MergeContainer adjacentContainer = adjacentNodeToContainer.get(adjacent);
            if (adjacentContainer == null) {
                adjacentNodeToContainer.put(adjacent, container);
            } else if (adjacentContainer != container) {
                adjacentNodeToContainer.remove(adjacent);
                borderingNodes.add(adjacent);
            }
        }
    }
}
