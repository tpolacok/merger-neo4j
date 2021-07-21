package eu.profinit.manta.graphplayground.repository.merger.common;


import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.EdgeProperty;
import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.MantaRelationshipType;
import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.NodeProperty;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.*;
import eu.profinit.manta.graphplayground.model.manta.core.EdgeIdentification;
import eu.profinit.manta.graphplayground.model.manta.merger.AbstractProcessorContext;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.repository.merger.connector.GraphOperation;
import eu.profinit.manta.graphplayground.repository.merger.connector.SuperRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.processor.MergerProcessor;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public abstract class TestGraphUtil {

    /**
     * Driver to the Neo4j.
     */
    protected Driver driver;

    public TestGraphUtil(Driver driver) {
        this.driver = driver;
    }

    public abstract List<MergerProcessorResult> merge(MergerProcessor processor, AbstractProcessorContext context,
                                             String[] object);

    public void createBasicMergeGraph(RevisionInterval revisionInterval) {
        driver.session().writeTransaction(tx -> {
            Node superRoot = new SuperRootHandler().getRoot(tx);
            // Root edge properties
            Map<String, Object> resourceRelProps = new HashMap<>();
            resourceRelProps.put(EdgeProperty.TRAN_START.t(), revisionInterval.getStart());
            resourceRelProps.put(EdgeProperty.TRAN_END.t(), revisionInterval.getEnd());
            resourceRelProps.put(EdgeProperty.CHILD_NAME.t(), new GraphOperation().processValueForChildName("Oracle DDL"));
            // Node properties
            Map<String, Object> resourceProps = new HashMap<>();
            resourceProps.put(NodeProperty.VERTEX_TYPE.t(), MantaNodeLabel.RESOURCE.toString());
            resourceProps.put(NodeProperty.RESOURCE_NAME.t(), "Oracle DDL");
            resourceProps.put(NodeProperty.RESOURCE_TYPE.t(), "Oracle scripts");
            resourceProps.put(NodeProperty.RESOURCE_DESCRIPTION.t(), "Oracle DDL scripts");
            // Parameters
            Map<String,Object> params = new HashMap<>();
            params.put("resourceProps", resourceProps);
            params.put("resourceRelProps", resourceRelProps);
            params.put("id", superRoot.id());

            tx.run("MATCH (superRoot) WHERE ID(superRoot)=$id\n" +
                    "   CREATE (superRoot)<-[:hasResource $resourceRelProps]-(res:Resource $resourceProps)", params);
            return null;
        });
    }

    /**
     * @param label Label of nodes.
     * @return Count of nodes having specific label
     */
    public int countNodesByLabel(MantaNodeLabel label) {
        return driver.session().writeTransaction(tx ->
                tx.run(String.format("MATCH (n:%s) RETURN COUNT(n)", label.getVertexLabel())).single().get(0).asInt());
    }

    public Map<String, Object> getRelationship(EdgeIdentification edgeId) {
        return driver.session().writeTransaction(tx -> {
            Result res = tx.run("OPTIONAL MATCH ()-[rel]->()\n" +
                            "   WHERE ID(rel)=$id\n" +
                            " RETURN rel",
                    Map.of("id", edgeId.getRelationId()));
            return res.hasNext() ? res.single().asMap() : Collections.emptyMap();
        });
    }

    /**
     * @param sourceId Source node id.
     * @param targetId Target node id.
     * @param type Relationship type.
     * @return Relationships between 2 nodes of given type.
     */
    public Map<String, Object> getRelationship(Long sourceId, Long targetId, MantaRelationshipType type) {
        return driver.session().writeTransaction(tx -> {
                Result res = tx.run(String.format(
                        "MATCH (source)-[rel:%s]->(target)\n" +
                                "     WHERE ID(source)=$idA AND ID(target)=$idB\n" +
                                "RETURN rel", type.t()),
                        Map.of("idA", sourceId, "idB", targetId));
                return res.hasNext() ? res.single().asMap() : Collections.emptyMap();
        });
    }

    /**
     * @param nodeId Node id.
     * @return Node attributes of a node.
     */
    public Map<String, Object> getAllNodeAttributes(Long nodeId) {
        return driver.session().writeTransaction(tx -> {
            Result res = tx.run("MATCH (node)-[rel:hasAttribute]->(attr)\n" +
                            "   WHERE ID(node)=$id \n" +
                            "RETURN attr,rel",
                    Map.of("id", nodeId));
            return res.hasNext() ? res.single().asMap() : Collections.emptyMap();
        });
    }

    /**
     * @param nodeId Node id.
     * @param attributeName Attribute name.
     * @param attributeValue Attribute value.
     * @return Node attributes of a node by a given properties.
     */
    public Map<String, Object> getNodeAttribute(Long nodeId, String attributeName, Object attributeValue) {
        return driver.session().writeTransaction(tx -> {
            Result res = tx.run("MATCH (node)-[rel:hasAttribute]->(attr)\n" +
                        "   WHERE ID(node)=$id AND attr.attributeName=$attributeName\n" +
                        "       AND attr.attributeValue=$attributeValue \n" +
                        "RETURN attr,rel",
                Map.of("id", nodeId,
                        "attributeName", attributeName,
                        "attributeValue", attributeValue));
            return res.hasNext() ? res.single().asMap() : Collections.emptyMap();
        });
    }

    /**
     * @param id Node id.
     * @param controlParentId Parent node id.
     * @return Node and its control relationship.
     */
    public Map<String, Object> getNodeById(Long id, Long controlParentId) {
        return driver.session().writeTransaction(tx ->
                tx.run("MATCH (node)-[rel:hasParent|hasResource]->(parent)\n" +
                                "   WHERE ID(node)=$id AND ID(parent)=$parentId RETURN node,rel",
                        Map.of("id", id, "parentId", controlParentId)).single().asMap());
    }

    /**
     * @param id Node id.
     * @param parentId Parent node id.
     * @param resourceId Resource node id.
     * @return Node and its parent and resource relationship.
     */
    public Map<String, Object> getNodeByIdParentResource(Long id, Long parentId, Long resourceId) {
        return driver.session().writeTransaction(tx -> {
           Result result = tx.run("MATCH (res)<-[relRes:hasResource]-(node)-[relParent:hasParent]->(parent)\n" +
                                "   WHERE ID(node)=$id AND ID(parent)=$parentId AND ID(res)=$resId\n" +
                                "   RETURN node,relRes,relParent",
                        Map.of("id", id, "parentId", parentId, "resId", resourceId));
            return result.hasNext() ? result.single().asMap() : Collections.emptyMap();
        });
    }

    /**
     * @param id Node id.
     * @return Resource with its control relationship and layer and layer's relationship.
     */
    public Map<String, Object> getResourceAndLayerByResourceId(Long id) {
        return driver.session().writeTransaction(tx ->
                tx.run("MATCH (layer)<-[relLayer:inLayer]-(res:Resource)-[relRes:hasResource]->()" +
                                " WHERE ID(res)=$id RETURN res,relRes,relLayer, layer",
                        Map.of("id", id)).single().asMap());
    }

    /**
     *
     * @param id Source code UUID.
     * @return Source code and its control relationship.
     */
    public Map<String, Object> getSourceCodeById(String id) {
        return driver.session().writeTransaction(tx ->
                tx.run("MATCH (node:SourceNode)<-[rel:hasSource]-() WHERE node.sourceNodeId=$id RETURN node,rel",
                        Map.of("id", id)).single().asMap());
    }

    /**
     * @param revisionInterval Revision interval.
     * @return List of relationships having specific revision interval.
     */
    public List<Relationship> getEdgesWithSpecificRevision(RevisionInterval revisionInterval) {
        return driver.session().writeTransaction(tx -> {
            Result result = tx.run("MATCH ()<-[rel]-()\n" +
                            "WHERE rel.tranStart=$tranStart AND rel.tranEnd=$tranEnd\n" +
                            "RETURN rel",
                    Map.of("tranStart", revisionInterval.getStart(),
                            "tranEnd", revisionInterval.getEnd()));
            return result.hasNext()? result.list(r -> r.get("rel").asRelationship()): Collections.emptyList();
        });
    }

    /**
     * @param nodeId Node id.
     * @return Relationships of the subtree of given node.
     */
    public List<Relationship> getSubtreeEdges(Long nodeId) {
        return driver.session().writeTransaction(tx -> {
            Result result = tx.run("MATCH p=(m)<-[:hasParent*]-()-[:hasAttribute*0..]->()\n" +
                            "WHERE ID(m)=$id\n" +
                            "UNWIND nodes(p) AS allNodes\n" +
                            "MATCH (allNodes)-[rels]-()\n" +
                            "return DISTINCT(rels)",
                    Map.of("id", nodeId));
            return result.hasNext()? result.list(r -> r.get("rels").asRelationship()): Collections.emptyList();
        });
    }

    /**
     * Compares relationship.
     * @param relationship Relationship.
     * @param revisionInterval Revision interval.
     * @param type Type of a relationship.
     * @param properties Expected relationship.
     */
    public void compareRelationship(Relationship relationship, RevisionInterval revisionInterval,
                                    MantaRelationshipType type, Map<String, Object> properties) {
        assertThat("Edge has correct type", relationship.type(), is(type.t()));
        properties.forEach((key, value) -> {
            assertThat(String.format("Property %s ", key),
                    relationship.get(key).asObject(), is(value));
        });
        compareRelationshipRevision(relationship, revisionInterval);
    }

    /**
     * Compares relationship.
     * @param relationship Relationship.
     * @param revisionInterval Revision interval.
     */
    public void compareRelationshipRevision(Relationship relationship, RevisionInterval revisionInterval) {
        assertThat("Incorrect revision start", relationship.get(EdgeProperty.TRAN_START.t()).asDouble(),
                is(revisionInterval.getStart()));
        assertThat("Incorrect revision end", relationship.get(EdgeProperty.TRAN_END.t()).asDouble(),
                is(revisionInterval.getEnd()));
    }

    /**
     * Compares source code.
     * @param sourceCodeProperties Expected properties.
     * @param sourceCode Source code node.
     */
    public void compareSourceCode(String[] sourceCodeProperties, Node sourceCode) {
        assertThat("Node is of Source Node label", sourceCode.labels(), hasItem(MantaNodeLabel.SOURCE_NODE.getVertexLabel()));
        assertThat("Incorrect source code name", sourceCode.get(NodeProperty.SOURCE_NODE_LOCAL.t()).asString(),
                is(sourceCodeProperties[SourceCodeFormat.LOCAL_NAME.getIndex()]));
        assertThat("Incorrect source code type", sourceCode.get(NodeProperty.SOURCE_NODE_HASH.t()).asString(),
                is(sourceCodeProperties[SourceCodeFormat.HASH.getIndex()]));
        assertThat("Incorrect source code description", sourceCode.get(NodeProperty.SOURCE_NODE_TECHNOLOGY.t()).asString(),
                is(sourceCodeProperties[SourceCodeFormat.TECHNOLOGY.getIndex()]));
        assertThat("Incorrect source code description", sourceCode.get(NodeProperty.SOURCE_NODE_CONNECTION.t()).asString(),
                is(sourceCodeProperties[SourceCodeFormat.CONNECTION.getIndex()]));
    }

    /**
     * Compares resource.
     * @param resourceProps Expected properties.
     * @param resource Resource node.
     */
    public void compareResource(String[] resourceProps, Node resource) {
        assertThat("Node is of Resource label", resource.labels(), hasItem(MantaNodeLabel.RESOURCE.getVertexLabel()));
        assertThat("Incorrect resource name", resource.get(NodeProperty.RESOURCE_NAME.t()).asString(),
                is(resourceProps[ResourceFormat.NAME.getIndex()]));
        assertThat("Incorrect resource type", resource.get(NodeProperty.RESOURCE_TYPE.t()).asString(),
                is(resourceProps[ResourceFormat.TYPE.getIndex()]));
        assertThat("Incorrect resource description", resource.get(NodeProperty.RESOURCE_DESCRIPTION.t()).asString(),
                is(resourceProps[ResourceFormat.DESCRIPTION.getIndex()]));
    }

    /**
     * Compares layer.
     * @param layerProps Expected properties.
     * @param layer Layer node.
     */
    public void compareLayer(String[] layerProps, Node layer) {
        assertThat("Node is of Layer label", layer.labels(), hasItem(MantaNodeLabel.LAYER.getVertexLabel()));
        assertThat("Incorrect layer name", layer.get(NodeProperty.LAYER_NAME.t()).asString(),
                is(layerProps[LayerFormat.NAME.getIndex()]));
        assertThat("Incorrect layer type", layer.get(NodeProperty.LAYER_TYPE.t()).asString(),
                is(layerProps[LayerFormat.TYPE.getIndex()]));
    }

    /**
     * Compares node.
     * @param nodeProps Expected properties.
     * @param node Node node.
     */
    public void compareNode(String[] nodeProps, Node node) {
        assertThat("Node is of Attribute label", node.labels(), hasItem(MantaNodeLabel.NODE.getVertexLabel()));
        assertThat("Incorrect node name", node.get(NodeProperty.NODE_NAME.t()).asString(),
                is(nodeProps[NodeFormat.NAME.getIndex()]));
        assertThat("Incorrect node type", node.get(NodeProperty.NODE_TYPE.t()).asString(),
                is(nodeProps[NodeFormat.TYPE.getIndex()]));
    }

    /**
     * Compares node attribute.
     * @param nodeProps Expected properties.
     * @param attribute Attribute node.
     */
    public void compareNodeAttribute(String[] nodeProps, Node attribute) {
        assertThat("Node is of Attribute label", attribute.labels(), hasItem(MantaNodeLabel.ATTRIBUTE.getVertexLabel()));
        assertThat("Incorrect node attribute name", attribute.get(NodeProperty.ATTRIBUTE_NAME.t()).asString(),
                is(nodeProps[AttributeFormat.KEY.getIndex()]));
        assertThat("Incorrect node attribute type", attribute.get(NodeProperty.ATTRIBUTE_VALUE.t()).asString(),
                is(nodeProps[AttributeFormat.VALUE.getIndex()]));
    }

}
