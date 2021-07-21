package eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.input;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats;
import eu.profinit.manta.graphplayground.model.manta.merger.parallel.input.identifiers.*;
import org.neo4j.graphdb.Direction;

/**
 * Represents merged file context.
 *
 * Contains methods for merging specific types - synchronization is done using concurrent maps.
 *
 * @author tpolacok.
 */
public class MergedFile {

    /**
     * Source codes list.
     */
    private final List<List<String>> sourceCodes = Collections.synchronizedList(new ArrayList<>());

    /**
     * Layers list.
     */
    private final List<List<String>> layers = Collections.synchronizedList(new ArrayList<>());

    /**
     * Resources list.
     */
    private final List<List<String>> resources = Collections.synchronizedList(new ArrayList<>());

    /**
     * Nodes list.
     */
    private final List<List<String>> nodes = Collections.synchronizedList(new ArrayList<>());

    /**
     * Node attributes list.
     */
    private final List<List<String>> nodeAttributes = Collections.synchronizedList(new ArrayList<>());

    /**
     * Edges list.
     */
    private final List<List<String>> edges = Collections.synchronizedList(new ArrayList<>());

    /**
     * Edge attributes list.
     */
    private final List<List<String>> edgeAttributes = Collections.synchronizedList(new ArrayList<>());

    /**
     * Layer identifier -> layer_id mapping.
     */
    private final Map<SourceCodeIdentifier, String> sourceCodeToId = new ConcurrentHashMap<>();

    /**
     * Layer identifier -> layer_id mapping.
     */
    private final Map<LayerIdentifier, String> layersToId = new ConcurrentHashMap<>();

    /**
     * Layer_id -> resource -> resource_id mappings.
     */
    private final Map<String, Map<ResourceIdentifier, String>> layerToResources = new ConcurrentHashMap<>();

    /**
     * Node_id -> node child -> child_id mappings.
     */
    private final Map<String, Map<NodeChildIdentifier, String>> nodesToChildrenId = new ConcurrentHashMap<>();

    /**
     * Node_id -> node attribute -> containing flag mappings.
     */
    private final Map<String, Map<NodeAttributeIdentifier, Boolean>> nodeToAttributes = new ConcurrentHashMap<>();

    /**
     * Node_id -> node_id -> edge identifier (type, direction) -> edge_id mappings.
     */
    private final Map<String, Map<String, Map<EdgeIdentifier, String>>> nodeToNodeEdgesId = new ConcurrentHashMap<>();

    /**
     * Layer to merged id mapping.
     */
    private final Map<String, Map<EdgeAttributeIdentifier, Boolean>> edgeToAttributes = new ConcurrentHashMap<>();

    /**
     * Layer, resource, node id generator.
     */
    private final AtomicInteger nodeIdGenerator = new AtomicInteger(0);

    /**
     * Edge id generator.
     */
    private final AtomicInteger edgeIdGenerator = new AtomicInteger(0);

    /**
     * Source code id generator.
     */
    private final AtomicInteger sourceCodeGenerator = new AtomicInteger(0);

    /**
     * @return List of all groups of merged objects.
     */
    public List<List<List<String>>> getMergeObjects() {
        return List.of(sourceCodes, layers, resources, nodes, nodeAttributes, edges, edgeAttributes);
    }

    /**
     * Retrieves merged edge id.
     * If edge did not exist before it is inserted with generated id.
     * @param edge Edge to be merged.
     * @param source Merged source node id.
     * @param target Merged target node id.
     * @param resource Resource node id.
     * @param type Type of edge.
     * @return Merged edge id.
     */
    public String computeEdge(List<String> edge, String source, String target, String resource, String type) {
        List<String> mergedEdge = new ArrayList<>(edge);
        mergedEdge.set(DataflowObjectFormats.EdgeFormat.SOURCE.getIndex(), source);
        mergedEdge.set(DataflowObjectFormats.EdgeFormat.TARGET.getIndex(), target);
        mergedEdge.set(DataflowObjectFormats.EdgeFormat.RESOURCE.getIndex(), resource);
        final boolean sourceHigherOrder = Integer.parseInt(source) > Integer.parseInt(target);
        Direction direction = sourceHigherOrder ? Direction.INCOMING : Direction.OUTGOING;
        String first = sourceHigherOrder ? target : source;
        String second = sourceHigherOrder ? source : target;
        EdgeIdentifier edgeIdentifier = new EdgeIdentifier(type, direction);
        return nodeToNodeEdgesId.computeIfAbsent(first, key -> new ConcurrentHashMap<>())
                .computeIfAbsent(second, key -> new ConcurrentHashMap<>())
                .computeIfAbsent(edgeIdentifier, key -> {
                    String id = String.valueOf(edgeIdGenerator.getAndIncrement());
                    mergedEdge.set(DataflowObjectFormats.EdgeFormat.ID.getIndex(), id);
                    edges.add(mergedEdge);
                    return id;
                });
    }

    /**
     * Merges edge attribute
     * @param edgeAttribute Edge attribute to merge.
     * @param edgeId Merged edge id.
     * @param edgeAttributeIdentifier Edge attribute identifier.
     */
    public void computeEdgeAttribute(List<String> edgeAttribute, String edgeId, EdgeAttributeIdentifier edgeAttributeIdentifier) {
        List<String> mergedEdgeAttribute = new ArrayList<>(edgeAttribute);
        edgeToAttributes.computeIfAbsent(edgeId, key -> new ConcurrentHashMap<>())
                .computeIfAbsent(edgeAttributeIdentifier, key -> {
                    mergedEdgeAttribute.set(DataflowObjectFormats.AttributeFormat.OBJECT_ID.getIndex(), edgeId);
                    edgeAttributes.add(mergedEdgeAttribute);
                    return true;
                });
    }

    /**
     * Retrieves merged node id.
     * If node did not exist before it is inserted with generated id.
     * @param node Node to be merged.
     * @param parent Merged node's parent id.
     * @param resource Merged node's resource id.
     * @param childIdentifier Node child identifier.
     * @return Merged node id.
     */
    public String computeNode(List<String> node, String parent, String resource, NodeChildIdentifier childIdentifier) {
        List<String> mergedNode= new ArrayList<>(node);
        mergedNode.set(DataflowObjectFormats.NodeFormat.PARENT_ID.getIndex(), parent);
        mergedNode.set(DataflowObjectFormats.NodeFormat.RESOURCE_ID.getIndex(), resource);
        String queried = parent.equals("") ? resource : parent;
        return nodesToChildrenId.computeIfAbsent(queried, key -> new ConcurrentHashMap<>())
                .computeIfAbsent(childIdentifier, key -> {
                    String id = String.valueOf(nodeIdGenerator.getAndIncrement());
                    mergedNode.set(DataflowObjectFormats.NodeFormat.ID.getIndex(), id);
                    nodes.add(mergedNode);
                    return id;
                });
    }

    /**
     * Merged node attribute.
     * @param nodeAttribute Node attribute to be merged.
     * @param nodeId Merged node id.
     * @param nodeAttributeIdentifier Node attribute identifier.
     */
    public void computeNodeAttribute(List<String> nodeAttribute, String nodeId, NodeAttributeIdentifier nodeAttributeIdentifier) {
        List<String> mergedNodeAttribute = new ArrayList<>(nodeAttribute);
        mergedNodeAttribute.set(DataflowObjectFormats.AttributeFormat.OBJECT_ID.getIndex(), nodeId);
        nodeToAttributes.computeIfAbsent(nodeId, key -> new ConcurrentHashMap<>())
                .computeIfAbsent(nodeAttributeIdentifier, key -> {
                    nodeAttributes.add(mergedNodeAttribute);
                    return true;
                });
    }

    /**
     * Merges layer.
     * If layer did not exist before it is inserted with generated id.
     * @param layer Layer to be merged.
     * @param layerIdentifier Layer identifier.
     * @return Merged layer id.
     */
    public String computeLayer(List<String> layer, LayerIdentifier layerIdentifier) {
        List<String> mergedLayer = new ArrayList<>(layer);
        return layersToId.computeIfAbsent(layerIdentifier, key -> {
            String id = String.valueOf(nodeIdGenerator.getAndIncrement());
            mergedLayer.set(DataflowObjectFormats.LayerFormat.ID.getIndex(), id);
            layers.add(mergedLayer);
            return id;
        });
    }

    /**
     * Merges resource.
     * If resource did not exist before it is inserted with generated id.
     * @param resource Resource to be merged.
     * @param layerId Merged layer id.
     * @param resourceIdentifier Resource identifier.
     * @return Merged resource id.
     */
    public String computeResource(List<String> resource, String layerId, ResourceIdentifier resourceIdentifier) {
        List<String> mergedResource = new ArrayList<>(resource);
        mergedResource.set(DataflowObjectFormats.ResourceFormat.LAYER.getIndex(), layerId);
        return layerToResources.computeIfAbsent(layerId, key -> new ConcurrentHashMap<>())
                .computeIfAbsent(resourceIdentifier, key -> {
                    String id = String.valueOf(nodeIdGenerator.getAndIncrement());
                    mergedResource.set(DataflowObjectFormats.ResourceFormat.ID.getIndex(), id);
                    resources.add(mergedResource);
                    return id;
                });
    }

    /**
     * Merges source code.
     * @param sourceCode Source code to be merged.
     * @return Merged source code id.
     */
    public String computeSourceCode(List<String> sourceCode, SourceCodeIdentifier sourceCodeIdentifier) {
        List<String> mergedSourceCode = new ArrayList<>(sourceCode);
        return sourceCodeToId.computeIfAbsent(sourceCodeIdentifier, key -> {
            String id = sourceCodeGenerator.incrementAndGet() + "";
            mergedSourceCode.set(DataflowObjectFormats.SourceCodeFormat.ID.getIndex(), id);
            sourceCodes.add(mergedSourceCode);
            return id;
        });
    }

    public List<List<String>> getSourceCodes() {
        return sourceCodes;
    }

    public List<List<String>> getLayers() {
        return layers;
    }

    public List<List<String>> getResources() {
        return resources;
    }

    public List<List<String>> getNodes() {
        return nodes;
    }

    public List<List<String>> getNodeAttributes() {
        return nodeAttributes;
    }

    public List<List<String>> getEdges() {
        return edges;
    }

    public List<List<String>> getEdgeAttributes() {
        return edgeAttributes;
    }
}
