package eu.profinit.manta.graphplayground.repository.merger.filemerger;

import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.*;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.input.FileMergerProcessor;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.input.InputFileContext;
import eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.input.MergedFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static eu.profinit.manta.graphplayground.repository.merger.common.CommonMergerTestObjects.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Tests for merger request unification.
 *
 * @author tpolacok
 */
public class FileMergerProcessorTest {

    /**
     * File merger processor.
     */
    private static FileMergerProcessor fileMergerProcessor;

    /**
     * Merged file.
     */
    private static MergedFile mergedFile;

    /**
     * Contexts for input request file.
     */
    private static InputFileContext fileContext;
    private static InputFileContext fileContext_2;

    @BeforeAll
    static void init() {
        fileMergerProcessor = new FileMergerProcessor();
    }

    @BeforeEach
    protected void beforeEachInit() {
        mergedFile = new MergedFile();
        fileContext = new InputFileContext();
        fileContext_2 = new InputFileContext();
    }

    @Test
    public void testMergeSourceCodeNew() {
        String initialSourceId = MERGED_SOURCE_CODE[SourceCodeFormat.ID.getIndex()];
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_SOURCE_CODE), mergedFile, fileContext);
        assertThat("Merged file has 1 source code", mergedFile.getSourceCodes(), hasSize(1));
        List<String> mergedSourceCode = mergedFile.getSourceCodes().get(0);
        assertThat("Input request file has the correct source code mapping",
                fileContext.getMappedSourceCode(initialSourceId),
                is(mergedSourceCode.get(SourceCodeFormat.ID.getIndex())));
    }

    @Test
    public void testMergeSourceCodeDuplicate() {
        String firstSourceCodeId = MERGED_SOURCE_CODE[SourceCodeFormat.ID.getIndex()];
        String[] second = Arrays.copyOf(MERGED_SOURCE_CODE, MERGED_SOURCE_CODE.length);
        String secondSourceCodeId = "22";
        second[SourceCodeFormat.ID.getIndex()] = secondSourceCodeId;

        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_SOURCE_CODE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(second), mergedFile, fileContext_2);

        assertThat("Merged file has 1 source code", mergedFile.getSourceCodes(), hasSize(1));
        List<String> mergedSourceCode = mergedFile.getSourceCodes().get(0);
        assertThat("First nput request file has the correct source code mapping",
                fileContext.getMappedSourceCode(firstSourceCodeId),
                is(mergedSourceCode.get(SourceCodeFormat.ID.getIndex())));
        assertThat("Second input request file has the correct source code mapping",
                fileContext_2.getMappedSourceCode(secondSourceCodeId),
                is(mergedSourceCode.get(SourceCodeFormat.ID.getIndex())));
    }

    @Test
    public void testMergeLayerNew() {
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext);

        assertThat("Merged file has 1 layer", mergedFile.getLayers(), hasSize(1));
        List<String> mergedLayer = mergedFile.getLayers().get(0);
        assertThat("Input request file has the layer mapping",
                fileContext.getMappedLayer(MERGED_LAYER[LayerFormat.ID.getIndex()]),
                is(mergedLayer.get(LayerFormat.ID.getIndex())));
    }

    @Test
    public void testMergeLayerDuplicate() {
        String[] second = Arrays.copyOf(MERGED_LAYER, MERGED_LAYER.length);
        String secondLayerId = "22";
        second[SourceCodeFormat.ID.getIndex()] = secondLayerId;

        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(second), mergedFile, fileContext_2);

        assertThat("Merged file has 1 layer", mergedFile.getLayers(), hasSize(1));
        List<String> mergedLayer = mergedFile.getLayers().get(0);
        assertThat("First input request file has the layer mapping",
                fileContext.getMappedLayer(MERGED_LAYER[LayerFormat.ID.getIndex()]),
                is(mergedLayer.get(LayerFormat.ID.getIndex())));

        assertThat("Second input request file has the layer mapping",
                fileContext_2.getMappedLayer(second[LayerFormat.ID.getIndex()]),
                is(mergedLayer.get(LayerFormat.ID.getIndex())));
    }

    @Test
    public void testMergeResourceNew() {
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_RESOURCE), mergedFile, fileContext);

        assertThat("Merged file has 1 resource", mergedFile.getResources(), hasSize(1));
        List<String> mergedResource = mergedFile.getResources().get(0);
        assertThat("Input request file has the layer mapping",
                fileContext.getMappedResource(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]),
                is(mergedResource.get(ResourceFormat.ID.getIndex())));
    }

    @Test
    public void testMergeResourceDuplicate() {
        String[] second = Arrays.copyOf(MERGED_RESOURCE, MERGED_RESOURCE.length);
        String secondResourceId = "22";
        second[ResourceFormat.ID.getIndex()] = secondResourceId;
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext_2);

        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(second), mergedFile, fileContext_2);

        assertThat("Merged file has 1 resource", mergedFile.getResources(), hasSize(1));
        List<String> mergedResource = mergedFile.getResources().get(0);
        assertThat("First input request file has the resource mapping",
                fileContext.getMappedResource(MERGED_RESOURCE[ResourceFormat.ID.getIndex()]),
                is(mergedResource.get(ResourceFormat.ID.getIndex())));
        assertThat("Second input request file has the resource mapping",
                fileContext_2.getMappedResource(second[ResourceFormat.ID.getIndex()]),
                is(mergedResource.get(ResourceFormat.ID.getIndex())));
    }

    @Test
    public void testMergeNodeNew() {
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_RESOURCE), mergedFile, fileContext);

        assertThat("Merged file has 1 node", mergedFile.getNodes(), hasSize(1));
        List<String> mergedNode = mergedFile.getNodes().get(0);
        List<String> mergedResource = mergedFile.getResources().get(0);
        assertThat("Input request file has the node mapping",
                fileContext.getMappedNode(MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()]),
                is(mergedNode.get(NodeFormat.ID.getIndex())));
        assertThat("Merged node has correct merged resource",
                mergedNode.get(NodeFormat.RESOURCE_ID.getIndex()),
                is(mergedResource.get(ResourceFormat.ID.getIndex())));
    }

    @Test
    public void testMergeNodeDuplicate() {
        String nodeId = MERGED_NODE_RESOURCE[NodeFormat.ID.getIndex()];
        String[] secondResource = Arrays.copyOf(MERGED_RESOURCE, MERGED_RESOURCE.length);
        String secondResourceId = "22";
        secondResource[ResourceFormat.ID.getIndex()] = secondResourceId;

        String[] secondNode = Arrays.copyOf(MERGED_NODE_RESOURCE, MERGED_NODE_RESOURCE.length);
        String secondNodeId = "132";
        secondNode[NodeFormat.ID.getIndex()] = secondNodeId;
        secondNode[NodeFormat.RESOURCE_ID.getIndex()] = secondResourceId;

        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_RESOURCE), mergedFile, fileContext);

        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(secondResource), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(secondNode), mergedFile, fileContext_2);

        assertThat("Merged file has 1 node", mergedFile.getNodes(), hasSize(1));
        List<String> mergedNode = mergedFile.getNodes().get(0);
        List<String> mergedResource = mergedFile.getResources().get(0);
        assertThat("First input request file has the node mapping",
                fileContext.getMappedNode(nodeId),
                is(mergedNode.get(NodeFormat.ID.getIndex())));
        assertThat("Second input request file has the node mapping",
                fileContext_2.getMappedNode(secondNodeId),
                is(mergedNode.get(NodeFormat.ID.getIndex())));
        assertThat("Merged node has correct merged resource",
                mergedNode.get(NodeFormat.RESOURCE_ID.getIndex()),
                is(mergedResource.get(ResourceFormat.ID.getIndex())));
    }

    @Test
    public void testMergeNodeAttributeNew() {
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_ATTRIBUTE), mergedFile, fileContext);

        assertThat("Merged file has 1 node attribute", mergedFile.getNodeAttributes(), hasSize(1));
        List<String> mergedNodeAttribute = mergedFile.getNodeAttributes().get(0);
        List<String> mergedNode = mergedFile.getNodes().get(0);
        assertThat("Merged node attribute has correct merged node ",
                mergedNodeAttribute.get(AttributeFormat.OBJECT_ID.getIndex()),
                is(mergedNode.get(NodeFormat.ID.getIndex())));
    }

    @Test
    public void testMergeNodeAttributeDuplicate() {
        String[] secondNode = Arrays.copyOf(MERGED_NODE_RESOURCE, MERGED_NODE_RESOURCE.length);
        String secondNodeId = "132";
        secondNode[NodeFormat.ID.getIndex()] = secondNodeId;

        String[] secondNodeAttribute = Arrays.copyOf(MERGED_NODE_ATTRIBUTE, MERGED_NODE_ATTRIBUTE.length);
        secondNodeAttribute[AttributeFormat.OBJECT_ID.getIndex()] = secondNodeId;

        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_ATTRIBUTE), mergedFile, fileContext);

        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_RESOURCE), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(secondNode), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(secondNodeAttribute), mergedFile, fileContext_2);

        assertThat("Merged file has 1 node attribute", mergedFile.getNodeAttributes(), hasSize(1));
        List<String> mergedNodeAttribute = mergedFile.getNodeAttributes().get(0);
        List<String> mergedNode = mergedFile.getNodes().get(0);
        assertThat("Merged node attribute has correct merged node ",
                mergedNodeAttribute.get(AttributeFormat.OBJECT_ID.getIndex()),
                is(mergedNode.get(NodeFormat.ID.getIndex())));
    }

    @Test
    public void testMergeNodeAttributeSourceMapping() {
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_SOURCE_CODE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_ATTRIBUTE_SOURCE_CODE), mergedFile, fileContext);

        assertThat("Merged file has 1 node attribute", mergedFile.getNodeAttributes(), hasSize(1));
        List<String> mergedNodeAttribute = mergedFile.getNodeAttributes().get(0);
        List<String> mergedNode = mergedFile.getNodes().get(0);
        List<String> mergedSourceCode = mergedFile.getSourceCodes().get(0);
        assertThat("Merged node attribute has correct merged node ",
                mergedNodeAttribute.get(AttributeFormat.OBJECT_ID.getIndex()),
                is(mergedNode.get(NodeFormat.ID.getIndex())));
        assertThat("Merged node attribute has correct merged source code value ",
                mergedNodeAttribute.get(AttributeFormat.VALUE.getIndex()),
                is(mergedSourceCode.get(SourceCodeFormat.ID.getIndex())));
    }

    @Test
    public void testMergeEdgeNew() {
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_SOURCE_CODE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_PARENT), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_EDGE), mergedFile, fileContext);

        assertThat("Merged file has 1 edge", mergedFile.getEdges(), hasSize(1));
        List<String> mergedEdge = mergedFile.getEdges().get(0);
        List<String> sourceNode = mergedFile.getNodes().get(0);
        List<String> targetNode = mergedFile.getNodes().get(1);
        assertThat("Merged edge has correct merged source node",
                mergedEdge.get(EdgeFormat.SOURCE.getIndex()),
                is(sourceNode.get(NodeFormat.ID.getIndex())));
        assertThat("Merged edge has correct merged target node",
                mergedEdge.get(EdgeFormat.TARGET.getIndex()),
                is(targetNode.get(NodeFormat.ID.getIndex())));
        assertThat("Input request file has the edge mapping",
                fileContext.getMappedEdge(MERGED_EDGE[EdgeFormat.ID.getIndex()]),
                is(mergedEdge.get(EdgeFormat.ID.getIndex())));
    }

    @Test
    public void testMergeEdgeDuplicate() {
        String[] secondEdge = Arrays.copyOf(MERGED_EDGE, MERGED_EDGE.length);
        String secondEdgeId = "132";
        secondEdge[EdgeFormat.ID.getIndex()] = secondEdgeId;

        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_SOURCE_CODE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_PARENT), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_EDGE), mergedFile, fileContext);

        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_SOURCE_CODE), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_RESOURCE), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_RESOURCE), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_PARENT), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(secondEdge), mergedFile, fileContext_2);

        assertThat("Merged file has 1 edge", mergedFile.getEdges(), hasSize(1));
        List<String> mergedEdge = mergedFile.getEdges().get(0);
        assertThat("First input request file has the edge mapping",
                fileContext.getMappedEdge(MERGED_EDGE[EdgeFormat.ID.getIndex()]),
                is(mergedEdge.get(EdgeFormat.ID.getIndex())));
        assertThat("First input request file has the edge mapping",
                fileContext_2.getMappedEdge(secondEdge[EdgeFormat.ID.getIndex()]),
                is(mergedEdge.get(EdgeFormat.ID.getIndex())));
    }

    @Test
    public void testMergeEdgeAttributeNew() {
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_SOURCE_CODE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_PARENT), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_EDGE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_EDGE_ATTRIBUTE), mergedFile, fileContext);

        assertThat("Merged file has 1 edge attribute", mergedFile.getEdgeAttributes(), hasSize(1));
        List<String> mergedEdgeAttribute = mergedFile.getEdgeAttributes().get(0);
        List<String> mergedEdge = mergedFile.getEdges().get(0);
        assertThat("Merged node attribute has correct merged node",
                mergedEdgeAttribute.get(AttributeFormat.OBJECT_ID.getIndex()),
                is(mergedEdge.get(EdgeFormat.ID.getIndex())));
    }

    @Test
    public void testMergeEdgeAttributeDuplicate() {
        String[] secondEdge = Arrays.copyOf(MERGED_EDGE, MERGED_EDGE.length);
        String secondEdgeId = "132";
        secondEdge[EdgeFormat.ID.getIndex()] = secondEdgeId;

        String[] secondEdgeAttribute = Arrays.copyOf(MERGED_EDGE_ATTRIBUTE, MERGED_EDGE_ATTRIBUTE.length);
        secondEdgeAttribute[AttributeFormat.OBJECT_ID.getIndex()] = secondEdgeId;

        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_SOURCE_CODE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_RESOURCE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_PARENT), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_EDGE), mergedFile, fileContext);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_EDGE_ATTRIBUTE), mergedFile, fileContext);

        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_SOURCE_CODE), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_LAYER), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_RESOURCE), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_RESOURCE), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(MERGED_NODE_PARENT), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(secondEdge), mergedFile, fileContext_2);
        fileMergerProcessor.mergeObject(Arrays.asList(secondEdgeAttribute), mergedFile, fileContext_2);

        assertThat("Merged file has 1 node attribute", mergedFile.getEdgeAttributes(), hasSize(1));
        List<String> mergedEdgeAttribute = mergedFile.getEdgeAttributes().get(0);
        List<String> mergedEdge = mergedFile.getEdges().get(0);
        assertThat("Merged edge attribute has correct merged edge",
                mergedEdgeAttribute.get(AttributeFormat.OBJECT_ID.getIndex()),
                is(mergedEdge.get(NodeFormat.ID.getIndex())));
    }

}
