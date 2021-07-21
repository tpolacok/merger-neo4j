package eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.input;

import java.util.HashMap;
import java.util.Map;

/**
 * Context for specific merge request for input merging.
 *
 * @author tpolacok
 */
public class InputFileContext {

    /**
     * Layer to merged layer mapping.
     */
    private Map<String, String> localLayerToMergedLayer = new HashMap<>();

    /**
     * Resource to merged resource mapping.
     */
    private Map<String, String> localResourceToMergedResource = new HashMap<>();

    /**
     * Node to merged node mapping.
     */
    private Map<String, String> localNodeToMergedNode = new HashMap<>();

    /**
     * Edge to merged edge mapping.
     */
    private Map<String, String> localEdgeToMergedEdge = new HashMap<>();

    /**
     * Edge to merged edge mapping.
     */
    private Map<String, String> localSourceCodeToMergedSourceCode = new HashMap<>();

    /**
     * Inserts layer mapping.
     * @param ownId Local layer id.
     * @param mergedId Merged layer id.
     */
    public void mapLayer(String ownId, String mergedId) {
        localLayerToMergedLayer.put(ownId, mergedId);
    }

    /**
     * Inserts resource mapping.
     * @param ownId Local resource id.
     * @param mergedId Merged resource id.
     */
    public void mapResource(String ownId, String mergedId) {
        localResourceToMergedResource.put(ownId, mergedId);
    }

    /**
     * Inserts node mapping.
     * @param ownId Local node id.
     * @param mergedId Merged node id.
     */
    public void mapNode(String ownId, String mergedId) {
        localNodeToMergedNode.put(ownId, mergedId);
    }

    /**
     * Inserts edge mapping.
     * @param ownId Local edge id.
     * @param mergedId Merged edge id.
     */
    public void mapEdge(String ownId, String mergedId) {
        localEdgeToMergedEdge.put(ownId, mergedId);
    }

    /**
     * Inserts source code mapping.
     * @param ownId Local source code id.
     * @param mergedId Merged source code id.
     */
    public void mapSourceCode(String ownId, String mergedId) {
        localSourceCodeToMergedSourceCode.put(ownId, mergedId);
    }

    /**
     * Gets merged layer id by local layer id.
     * @param ownId Local layer id.
     * @return Merged layer id.
     */
    public String getMappedLayer(String ownId) {
        return localLayerToMergedLayer.get(ownId);
    }

    /**
     * Gets merged resource id by local resource id.
     * @param ownId Local resource id.
     * @return Merged resource id.
     */
    public String getMappedResource(String ownId) {
        return localResourceToMergedResource.get(ownId);
    }

    /**
     * Gets merged node id by local node id.
     * @param ownId Local node id.
     * @return Merged node id.
     */
    public String getMappedNode(String ownId) {
        return localNodeToMergedNode.getOrDefault(ownId, "");
    }

    /**
     * Gets merged edge id by local edge id.
     * @param ownId Local edge id.
     * @return Merged edge id.
     */
    public String getMappedEdge(String ownId) {
        return localEdgeToMergedEdge.get(ownId);
    }

    /**
     * Gets merged source code id by local source code id.
     * @param ownId Local source code id.
     * @return Merged source code id.
     */
    public String getMappedSourceCode(String ownId) {
        return localSourceCodeToMergedSourceCode.get(ownId);
    }
}
