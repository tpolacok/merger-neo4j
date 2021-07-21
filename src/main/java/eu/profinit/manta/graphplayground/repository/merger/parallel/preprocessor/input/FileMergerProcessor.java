package eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.input;

import java.util.ArrayList;
import java.util.List;

import eu.profinit.manta.graphplayground.model.manta.merger.parallel.input.identifiers.*;
import eu.profinit.manta.graphplayground.repository.merger.processor.StandardMergerProcessor;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats;

/**
 * File processor merger.
 *
 * @author tpolacok
 */
public class FileMergerProcessor {

    /**
     * Merges object into merged file context.
     * @param mergeObject Object to merge.
     * @param mergedFile Shared merged file context.
     * @param fileContext Request file context.
     */
    public void mergeObject(List<String> mergeObject, MergedFile mergedFile, InputFileContext fileContext ) {
        DataflowObjectFormats.ItemTypes itemType = DataflowObjectFormats.ItemTypes.parseString(mergeObject.get(0));
        switch (itemType) {
            case LAYER: {
                mergeLayer(mergeObject, mergedFile, fileContext);
                break;
            }
            case NODE: {
                mergeNode(mergeObject, mergedFile, fileContext);
                break;
            }
            case EDGE: {
                mergeEdge(mergeObject, mergedFile, fileContext);
                break;
            }
            case RESOURCE: {
                mergeResource(mergeObject, mergedFile, fileContext);
                break;
            }
            case NODE_ATTRIBUTE: {
                mergeNodeAttribute(mergeObject, mergedFile, fileContext);
                break;
            }
            case EDGE_ATTRIBUTE: {
                mergeEdgeAttribute(mergeObject, mergedFile, fileContext);
                break;
            }
            case SOURCE_CODE: {
                mergeSourceCode(mergeObject, mergedFile, fileContext);
                break;
            }
            default:
        }
    }

    /**
     * Merges node into merged file context.
     * @param node Node to merge.
     * @param mergedFile Shared merged file context.
     * @param fileContext Request file context.
     */
    private void mergeNode(List<String> node, MergedFile mergedFile, InputFileContext fileContext) {
        String localId = node.get(DataflowObjectFormats.NodeFormat.ID.getIndex());
        String parent = fileContext.getMappedNode(node.get(DataflowObjectFormats.NodeFormat.PARENT_ID.getIndex()));
        String resource = fileContext.getMappedResource(node.get(DataflowObjectFormats.NodeFormat.RESOURCE_ID.getIndex()));
        NodeChildIdentifier childIdentifier =
                new NodeChildIdentifier(node.get(DataflowObjectFormats.NodeFormat.NAME.getIndex()), node.get(DataflowObjectFormats.NodeFormat.TYPE.getIndex()));
        String mergedId = mergedFile.computeNode(node, parent, resource, childIdentifier);
        fileContext.mapNode(localId, mergedId);
    }

    /**
     * Merges edge into merged file context.
     * @param edge Edge to merge.
     * @param mergedFile Shared merged file context.
     * @param fileContext Request file context.
     */
    private void mergeEdge(List<String> edge, MergedFile mergedFile, InputFileContext fileContext) {
        String localId = edge.get(DataflowObjectFormats.EdgeFormat.ID.getIndex());
        String source = fileContext.getMappedNode(edge.get(DataflowObjectFormats.EdgeFormat.SOURCE.getIndex()));
        String target = fileContext.getMappedNode(edge.get(DataflowObjectFormats.EdgeFormat.TARGET.getIndex()));
        String resource = fileContext.getMappedResource(edge.get(DataflowObjectFormats.EdgeFormat.RESOURCE.getIndex()));
        String type = edge.get(DataflowObjectFormats.EdgeFormat.TYPE.getIndex());

        String mergedId = mergedFile.computeEdge(edge, source, target, resource, type);
        fileContext.mapEdge(localId, mergedId);
    }

    /**
     * Merges layer into merged file context.
     * @param layer Edge to merge.
     * @param mergedFile Shared merged file context.
     * @param fileContext Request file context.
     */
    private void mergeLayer(List<String> layer, MergedFile mergedFile, InputFileContext fileContext) {
        String localId = layer.get(DataflowObjectFormats.LayerFormat.ID.getIndex());
        LayerIdentifier layerIdentifier =
                new LayerIdentifier(layer.get(DataflowObjectFormats.LayerFormat.NAME.getIndex()), layer.get(DataflowObjectFormats.LayerFormat.TYPE.getIndex()));
        String mergedId = mergedFile.computeLayer(layer, layerIdentifier);
        fileContext.mapLayer(localId, mergedId);
    }

    /**
     * Merges resource into merged file context.
     * @param resource Edge to merge.
     * @param mergedFile Shared merged file context.
     * @param fileContext Request file context.
     */
    private void mergeResource(List<String> resource, MergedFile mergedFile, InputFileContext fileContext) {
        String localId = resource.get(DataflowObjectFormats.ResourceFormat.ID.getIndex());
        String layer = fileContext.getMappedLayer(resource.get(DataflowObjectFormats.ResourceFormat.LAYER.getIndex()));
        ResourceIdentifier resourceIdentifier =
                new ResourceIdentifier(resource.get(DataflowObjectFormats.ResourceFormat.NAME.getIndex()),
                        resource.get(DataflowObjectFormats.ResourceFormat.TYPE.getIndex()),
                        resource.get(DataflowObjectFormats.ResourceFormat.DESCRIPTION.getIndex()));
        String mergedId = mergedFile.computeResource(resource, layer, resourceIdentifier);
        fileContext.mapResource(localId, mergedId);
    }

    /**
     * Merges source code into merged file context.
     * @param sourceCode Edge to merge.
     * @param mergedFile Shared merged file context.
     */
    private void mergeSourceCode(List<String> sourceCode, MergedFile mergedFile, InputFileContext fileContext) {
        String localId = sourceCode.get(DataflowObjectFormats.SourceCodeFormat.ID.getIndex());
        SourceCodeIdentifier sourceCodeIdentifier = new SourceCodeIdentifier(
                sourceCode.get(DataflowObjectFormats.SourceCodeFormat.LOCAL_NAME.getIndex()),
                sourceCode.get(DataflowObjectFormats.SourceCodeFormat.HASH.getIndex()),
                sourceCode.get(DataflowObjectFormats.SourceCodeFormat.TECHNOLOGY.getIndex()),
                sourceCode.get(DataflowObjectFormats.SourceCodeFormat.CONNECTION.getIndex())
        );
        String mergedId = mergedFile.computeSourceCode(sourceCode, sourceCodeIdentifier);
        fileContext.mapSourceCode(localId, mergedId);
    }

    /**
     * Merges node attribute into merged file context.
     * @param nodeAttribute Edge to merge.
     * @param mergedFile Shared merged file context.
     * @param fileContext Request file context.
     */
    private void mergeNodeAttribute(List<String> nodeAttribute, MergedFile mergedFile, InputFileContext fileContext) {
        String node = fileContext.getMappedNode(nodeAttribute.get(DataflowObjectFormats.AttributeFormat.OBJECT_ID.getIndex()));
        List<String> mergedNodeAttribute = new ArrayList<>(nodeAttribute);
        if (nodeAttribute.get(DataflowObjectFormats.AttributeFormat.KEY.getIndex()).equals(StandardMergerProcessor.SOURCE_LOCATION)) {
            mergedNodeAttribute.set(DataflowObjectFormats.AttributeFormat.VALUE.getIndex(), fileContext.getMappedSourceCode(
                    nodeAttribute.get(DataflowObjectFormats.AttributeFormat.VALUE.getIndex()
                    )));
        }
        NodeAttributeIdentifier nodeAttributeIdentifier
                = new NodeAttributeIdentifier(nodeAttribute.get(DataflowObjectFormats.AttributeFormat.KEY.getIndex()),
                        nodeAttribute.get(DataflowObjectFormats.AttributeFormat.VALUE.getIndex()));
        mergedFile.computeNodeAttribute(mergedNodeAttribute, node, nodeAttributeIdentifier);
    }

    /**
     * Merges edge attribute into merged file context.
     * @param edgeAttribute Edge to merge.
     * @param mergedFile Shared merged file context.
     * @param fileContext Request file context.
     */
    private void mergeEdgeAttribute(List<String> edgeAttribute, MergedFile mergedFile, InputFileContext fileContext) {
        String edge = fileContext.getMappedEdge(edgeAttribute.get(DataflowObjectFormats.AttributeFormat.OBJECT_ID.getIndex()));
        EdgeAttributeIdentifier edgeAttributeIdentifier
                = new EdgeAttributeIdentifier(edgeAttribute.get(DataflowObjectFormats.AttributeFormat.KEY.getIndex()),
                        edgeAttribute.get(DataflowObjectFormats.AttributeFormat.VALUE.getIndex()));
        mergedFile.computeEdgeAttribute(edgeAttribute, edge, edgeAttributeIdentifier);
    }
}
