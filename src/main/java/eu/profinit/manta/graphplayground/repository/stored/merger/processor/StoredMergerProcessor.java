package eu.profinit.manta.graphplayground.repository.stored.merger.processor;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.EdgeProperty;
import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.MantaRelationshipType;
import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.NodeProperty;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.*;
import eu.profinit.manta.graphplayground.model.manta.core.EdgeIdentification;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.ProcessingResult.ResultType;
import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.model.manta.merger.SourceCodeMapping;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.model.manta.util.Base64AttributeCodingHelper;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedEdgeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedNodeAttributeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedNodeResourceEdgeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedPerspectiveEdgeQuery;
import eu.profinit.manta.graphplayground.repository.merger.processor.MergerProcessor;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphCreation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphOperation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSourceRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSuperRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * Inner merger processor running within a single transaction. Full update and incremental update follow the same merging
 * process. The only exception is in the node flag - incremental update uses flags on nodes for additional operations during
 * the merge process.
 * <p>
 * If a node has flag REMOVE, then the whole subtree, starting from this specially marked node, is removed. That means,
 * all vertices and edges in this subtree (including data flows) have their tranEnd property set to the latest committed revision number.
 * After the subgraph removal is completed, merging continues from this specially marked node.
 * <p>
 * If the equivalent object of the merged object does NOT exist in the latest committed revision, then the object is created
 * in the new revision. Start revision (tranStart) is the new revision. End revision (tranEnd) is the maximum minor revision
 * of the new revision (major.999999).
 *
 * Example of full update (latestCommittedRevision = 1.012345, newMajorRevision=2.000000)
 *
 *    (Before merge)        merge into           (After merge)
 *      <N/A, N/A>       revision 2.000000    <2.000000, 2.999999>
 *     non existing           ------>              mergedObject
 *
 * Example of incremental update (latestCommittedRevision = 1.012345, newMinorRevision=1.012346)
 *
 *    (Before merge)        merge into           (After merge)
 *      <N/A, N/A>       revision 1.012346    <1.012346, 1.999999>
 *     non existing           ------>              mergedObject
 *
 * <p>
 * If the equivalent object of the merged object exists in the latest committed revision, then its end revision is set to the
 * maximum minor revision of the new revision (major.999999). Additionally, subtree removal may be performed during incremental
 * update.
 *
 * Example of full update (latestCommittedRevision = 1.012345, newMajorRevision=2.000000)
 *
 *    (Before merge)         merge into           (After merge)
 * <1.011111, 1.999999>   revision 2.000000    <1.011111, 2.999999>
 *      mergedObject           ------>             mergedObject
 *
 * Example of incremental update (latestCommittedRevision = 1.012345, newMinorRevision=1.012346)
 *
 *    (Before merge)         merge into           (After merge)
 * <1.011111, 1.999999>   revision 1.012346    <1.011111, 1.999999>
 *      mergedObject          ------>              mergedObject
 *
 * Example of incremental update + subtree removal (latestCommittedRevision = 1.012345, newMinorRevision=1.012346)
 *
 *    (Before merge)       subtree    (removed in subtree)       merge into          (After merge)
 * <1.011111, 1.999999>    removal    <1.011111, 1.012345>   revision 1.012346   <1.011111, 1.999999>
 *      mergedObject       ------>        mergedObject            ------>             mergedObject
 *
 * @author tfechtner
 * @author jsykora
 * @author tpolacok
 */
public class StoredMergerProcessor implements MergerProcessor<StoredProcessorContext> {

    protected Log LOGGER;

    /**
     * Forbidden prefix of edge attribute in Titan - TODO no longer needed in neo4j
     */
    protected static final String LABEL_KEY_WORD = "label";

    /**
     * Source location attribute key.
     */
    public static final String SOURCE_LOCATION = "sourceLocation";

    /**
     * MapsTo edge between source and target vertices.
     */
    public static final String MAPS_TO = "mapsTo";

    /**
     * Database access class for retrieving database objects.
     */
    protected final StoredGraphOperation graphOperation;

    /**
     * Database access class for creating database objects.
     */
    protected final StoredGraphCreation graphCreation;

    /**
     * Database access class for working with revisions.
     */
    protected final StoredRevisionUtils revisionUtils;

    /**
     * Delimiter for logging merged objects.
     */
    protected static final String DELIMITER = ",";

    /**
     * Holder for super root
     */
    protected final StoredSuperRootHandler superRootHandler;
    /**
     * Holder for source root
     */
    protected final StoredSourceRootHandler sourceRootHandler;
    /**
     * Revision root handler for the management of revision tree
     */
    protected final StoredRevisionRootHandler revisionRootHandler;

    public StoredMergerProcessor(StoredSuperRootHandler superRootHandler, StoredSourceRootHandler sourceRootHandler,
                                 StoredRevisionRootHandler revisionRootHandler, StoredGraphOperation graphOperation,
                                 StoredGraphCreation graphCreation, StoredRevisionUtils revisionUtils, Log logger) {
        this.superRootHandler = superRootHandler;
        this.sourceRootHandler = sourceRootHandler;
        this.revisionRootHandler = revisionRootHandler;
        this.graphOperation = graphOperation;
        this.graphCreation = graphCreation;
        this.revisionUtils = revisionUtils;
        this.LOGGER = logger;
    }

    /**
     * Merges object based on its type.
     * @param itemParameters Parameters of the merged object.
     * @param context Local to database context.
     * @return List of merging results.
     */
    @Override
    public List<MergerProcessorResult> mergeObject(String[] itemParameters, StoredProcessorContext context) {
        ItemTypes itemType = ItemTypes.parseString(itemParameters[0]);
        if (itemType != null) {
            switch (itemType) {
                case LAYER:
                    // Vrstvu si odlozime do pameti a zpracujeme ji az s resourcem
                    preprocessLayer(itemParameters, context);
                    return new ArrayList<>();
                case NODE:
                    return Collections
                            .singletonList(new MergerProcessorResult(processNode(itemParameters, context), itemType));
                case EDGE:
                    Map<ItemTypes, ResultType> processEdgeResult = processEdge(itemParameters, context);
                    List<MergerProcessorResult> edgeResult = new ArrayList<>();
                    for (Entry<ItemTypes, ResultType> entry : processEdgeResult.entrySet()) {
                        edgeResult.add(new MergerProcessorResult(entry.getValue(), entry.getKey()));
                    }
                    return edgeResult;
                case RESOURCE:
                    // Ocekavame obecne vice mergeovanych prvku: resource, vrstvu a hranu mezi nimi
                    Map<ItemTypes, ResultType> processResourceResult = processResource(itemParameters, context);
                    List<MergerProcessorResult> result = new ArrayList<>();
                    for (Entry<ItemTypes, ResultType> entry : processResourceResult.entrySet()) {
                        result.add(new MergerProcessorResult(entry.getValue(), entry.getKey()));
                    }
                    return result;
                case NODE_ATTRIBUTE:
                    return Collections.singletonList(processNodeAttribute(itemParameters, context));
                case EDGE_ATTRIBUTE:
                    return Collections.singletonList(
                            new MergerProcessorResult(processEdgeAttribute(itemParameters, context), itemType));
                case SOURCE_CODE:
                    return Collections
                            .singletonList(new MergerProcessorResult(processSourceCode(itemParameters, context), itemType));
                default:
                    LOGGER.warn(String.format("Unknown item type %s.", itemParameters[0]));
                    return Collections.singletonList(new MergerProcessorResult(ResultType.ERROR, null));
            }
        } else {
            LOGGER.warn(String.format("Unknown item type %s.", itemParameters[0]));
            return Collections.singletonList(new MergerProcessorResult(ResultType.ERROR, null));
        }
    }

    /**
     * Preprocesses the layer before its merged with the resource.
     * The layer is only inserted to the contextual object for fast retrieval.
     *
     * @param itemParameters Layer's merge parameters.
     * @param context Merger context.
     */
    protected void preprocessLayer(String[] itemParameters, StoredProcessorContext context) {
        if (itemParameters.length == ItemTypes.LAYER.getSize()) {
            String layerId = itemParameters[LayerFormat.ID.getIndex()];
            context.getMapLayerIdToLayer().put(layerId, itemParameters);
        } else {
            LOGGER.warn(String.format("Incorrect layer record: %s", StringUtils.join(itemParameters, DELIMITER)));
        }
    }

    /**
     * Processes (merge) resource. Together with resource is processed (merged) its layer.
     *
     * If resource does not exist, a new resource vertex is created,
     * {@link DatabaseStructure.MantaRelationshipType#HAS_RESOURCE} edge is created and
     * connected from resource to the super root and the revision validity
     * of the resource is set to the new revision.
     *
     * <br><br>
     *
     * If resource exists, its revision validity is increased up to the new revision.
     * <ul>
     *     <li>
     *          If resource's layer does not exist, then a new layer vertex is created,
     *          {@link DatabaseStructure.MantaRelationshipType#IN_LAYER} edge is created and
     *          connected from resource to the layer, and the validity revision
     *          of the layer is set to the new revision.
     *     </li>
     *     <li>
     *          If resource's layer exists, then the layer's revision validity is updated up to the new revision.
     *     </li>
     * </ul>
     *
     * Context updates:
     * <ul>
     *     <li>
     *         If the resource exists.
     *         <ul>
     *             <li>
     *                 Mapping of its local identifier to the database identifier is stored.
     *             </li>
     *             <li>
     *                 Identity mapping of its database identifier is stored for later node processing.
     *             </li>
     *             <li>
     *                 Resource database identifier is added to nodes existed set.
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         If a new resource is created.
     *         <ul>
     *             <li>
     *                 Mapping of its local identifier to the database identifier is stored.
     *             </li>
     *             <li>
     *                 Identity mapping of its database identifier is stored for later node processing.
     *             </li>
     *         </ul>
     *     </li>
     * </ul>
     *
     * @param itemParameters  Resource's merge parameters.
     * @param context  Merger context.
     * @return Result of the merging.
     */
    protected Map<ItemTypes, ResultType> processResource(String[] itemParameters, StoredProcessorContext context) {
        Map<ItemTypes, ResultType> result = new LinkedHashMap<>();
        if (itemParameters.length == ItemTypes.RESOURCE.getSize()) {
            String resId = itemParameters[ResourceFormat.ID.getIndex()];
            String resName = itemParameters[ResourceFormat.NAME.getIndex()];
            String resType = itemParameters[ResourceFormat.TYPE.getIndex()];
            String resDescription = itemParameters[ResourceFormat.DESCRIPTION.getIndex()];
            String layerLocalId = itemParameters[ResourceFormat.LAYER.getIndex()];

            Node root = superRootHandler.getRoot(context.getServerDbTransaction());

            Double newRevision = context.getRevision();
            Double newEndRevision = StoredRevisionUtils.getMaxMinorRevisionNumber(newRevision);
            Double latestCommittedRevision = context.getLatestCommittedRevision();

            // Najdeme vrstvu
            String[] layer = context.getMapLayerIdToLayer().get(layerLocalId);
            if (layer == null) {
                LOGGER.warn(String.format("For resource %s does not exist layer", StringUtils.join(itemParameters, DELIMITER)));
                result.put(ItemTypes.LAYER, ResultType.ERROR);
                result.put(ItemTypes.RESOURCE, ResultType.ERROR);
                return result;
            }
            List<Node> nodes = graphOperation.getChildren(context.getServerDbTransaction(), root.getId(), resName, resType,
                    MantaRelationshipType.HAS_RESOURCE.t(), NodeProperty.RESOURCE_NAME.t(), NodeProperty.RESOURCE_TYPE.t(), latestCommittedRevision);
            Node existingResourceVertex = null;
            if (!nodes.isEmpty()) {
                existingResourceVertex = nodes.get(0);
                context.getMapResourceIdToDbId().put(resId, existingResourceVertex.getId());
                revisionUtils.setVertexTransactionEnd(context.getServerDbTransaction(), existingResourceVertex, newEndRevision);
                context.getMapNodeDbIdResourceDbId().put(existingResourceVertex.getId(), existingResourceVertex.getId());
                context.getNodesExistedBefore().add(existingResourceVertex.getId());
            }

            String layerName = layer[LayerFormat.NAME.getIndex()];
            String layerType = layer[LayerFormat.TYPE.getIndex()];

            if (existingResourceVertex == null) {
                // Pokud neexistuje resource, vytvorime uzel resourcu a vrstvy a propojime je
                LOGGER.debug("Resource does not exist => new one will be created.");
                Node newLayerVertex = graphCreation.createLayer(context.getServerDbTransaction(), layerName, layerType);
                Node newResourceVertex = graphCreation.createResource(context.getServerDbTransaction(), root, resName,
                        resType, resDescription, newLayerVertex, new RevisionInterval(newRevision, newEndRevision));
                context.getMapResourceIdToDbId().put(resId, newResourceVertex.getId());
                context.getMapNodeDbIdResourceDbId().put(newResourceVertex.getId(), newResourceVertex.getId());
                result.put(ItemTypes.LAYER, ResultType.NEW_OBJECT);
                result.put(ItemTypes.RESOURCE, ResultType.NEW_OBJECT);
                return result;
            } else {
                // vime ze resource existuje, ale musime zjistit, zda existuje i jeho vrstva
                List<Node> layers = graphOperation.getLayer(context.getServerDbTransaction(), existingResourceVertex,
                        layerName, latestCommittedRevision);
                boolean layerNotExistsYet = true;
                if (!layers.isEmpty()) {
                    Node layerVertex = layers.get(0);
                    layerNotExistsYet = false;
                    String checkedLayerVertexType = layerVertex.getProperty(NodeProperty.LAYER_TYPE.t()).toString();
                    if (!checkedLayerVertexType.equals(layerType)) {
                        LOGGER.warn(String.format("Layer with name '%s' already exists, but the type is different."
                                        + " The existing type '%s' is used and the new type '%s' is ignored.", layerName,
                                checkedLayerVertexType, layerType));
                    }
                    revisionUtils.setVertexTransactionEnd(context.getServerDbTransaction(), layerVertex, newEndRevision);
                }
                if (layerNotExistsYet) {
                    // Vrstva neexistuje => vyrobime ji
                    LOGGER.debug("Layer does not exist => new one will be created.");
                    Node layerVertex = graphCreation.createLayer(context.getServerDbTransaction(), layerName, layerType);
                    graphCreation.createControlEdge(context.getServerDbTransaction(), existingResourceVertex, layerVertex,
                            MantaRelationshipType.IN_LAYER.t(), newRevision, newEndRevision);
                    result.put(ItemTypes.LAYER, ResultType.NEW_OBJECT);
                } else {
                    // Vrstva pro resource jiz existuje => neni co resit
                    LOGGER.debug("Layer already exists => nothing to be done");
                    result.put(ItemTypes.LAYER, ResultType.ALREADY_EXIST);
                }
                result.put(ItemTypes.RESOURCE, ResultType.ALREADY_EXIST);
                return result;
            }
        } else {
            LOGGER.warn(String.format("Incorrect resource record: %s", StringUtils.join(itemParameters, DELIMITER)));
            result.put(ItemTypes.LAYER, ResultType.ERROR);
            result.put(ItemTypes.RESOURCE, ResultType.ERROR);
            return result;
        }
    }

    /**
     * Processes node.
     * <br>
     * If the node does not exist it is created:
     *      <ul>
     *          <li>
     *              If a node has node parent but no resource, its resource is inherited,
     *              but the control edge is its {@link DatabaseStructure.MantaRelationshipType#HAS_PARENT} relationship.
     *          </li>
     *          <li>
     *              If a node has no parent node the edge between node and its resource is
     *              created ({@link DatabaseStructure.MantaRelationshipType#HAS_PARENT})and serves as the control edge
     *              </li>
     *          <li>
     *              If a node has both parent node and resource, which is different from its parent,
     *              the edges connecting it to both parent and resources are created
     *          </li>
     *      </ul>
     * If the node exists its revision is updated.
     *
     * <ul>
     *     <li>
     *         If the merge parameters contain special flag {@link DataflowObjectFormats.Flag#REMOVE},
     *         revisions of its subtree and its control edge are closed to latest committed revision.
     *     </li>
     *     <li>
     *         If the merge parameters contain special flag {@link DataflowObjectFormats.Flag#REMOVE_MYSELF},
     *         revisions of its subtree are closed to latest committed revision.
     *     </li>
     * </ul>
     *
     * Context updates:
     * <ul>
     *     <li>
     *         When the node did exist prior to merging.
     *         <ul>
     *             <li>
     *                 Mapping of its local identifier to the database identifier is stored.
     *             </li>
     *             <li>
     *                 Mapping of its node database identifier to its resource database
     *                 identifier is stored for later node processing.
     *             </li>
     *             <li>
     *                 Node database identifier is added to nodes existed set.
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         When new node is created.
     *         <ul>
     *             <li>
     *                 Mapping of its local identifier to the database identifier is stored.
     *             </li>
     *             <li>
     *                 Mapping of its node database identifier to its resource database
     *                 identifier is stored for later node processing.
     *             </li>
     *         </ul>
     *     </li>
     * </ul>
     *
     * @param itemParameters  Node's merge parameters.
     * @param context  Merger context.
     * @return Result of the merging.
     */
    protected ResultType processNode(String[] itemParameters, StoredProcessorContext context) {
        if (itemParameters.length == ItemTypes.NODE.getSize()
                || itemParameters.length == ItemTypes.NODE.getSize() - 1) {
            String nodeId = itemParameters[NodeFormat.ID.getIndex()];
            String nodeType = itemParameters[NodeFormat.TYPE.getIndex()];
            String name = itemParameters[NodeFormat.NAME.getIndex()];
            String resourceId = itemParameters[NodeFormat.RESOURCE_ID.getIndex()];
            String parentLocalId = itemParameters[NodeFormat.PARENT_ID.getIndex()];
            String flag = null;
            if (itemParameters.length == ItemTypes.NODE.getSize()) {
                flag = itemParameters[NodeFormat.FLAG.getIndex()];
            }
            Double newRevision = context.getRevision();
            Double newEndRevision = StoredRevisionUtils.getMaxMinorRevisionNumber(newRevision);
            Double latestCommittedRevision = context.getLatestCommittedRevision();
            Long resourceVertexId = context.getMapResourceIdToDbId().get(resourceId);
            if (resourceVertexId == null) {
                LOGGER.warn(String.format("For node %s does not exist resource",
                        StringUtils.join(itemParameters, DELIMITER)));
                return ResultType.ERROR;
            }
            Long parentVertexId;
            MantaRelationshipType parentEdgeType;
            if (parentLocalId == null || parentLocalId.isEmpty()) {
                parentEdgeType = MantaRelationshipType.HAS_RESOURCE;
                parentVertexId = resourceVertexId;
            } else {
                parentEdgeType = MantaRelationshipType.HAS_PARENT;
                parentVertexId = context.getMapNodeIdToDbId().get(parentLocalId);
                if (parentVertexId == null) {
                    LOGGER.warn(String.format("For node %s does not exist parent.",
                            StringUtils.join(itemParameters, DELIMITER)));
                    return ResultType.ERROR;
                }
            }
            boolean notYetExist = true;
            if (context.getNodesExistedBefore().contains(parentVertexId)) {
                List<Node> children = graphOperation.getChildren(context.getServerDbTransaction(), parentVertexId, name, nodeType,
                        parentEdgeType.t(), NodeProperty.NODE_NAME.t(), NodeProperty.NODE_TYPE.t(), latestCommittedRevision);
                if (!children.isEmpty()) {
                    Node child = children.get(0);
                    context.getNodesExistedBefore().add(child.getId());
                    context.getMapNodeIdToDbId().put(nodeId, child.getId());
                    context.getMapNodeDbIdResourceDbId().put(child.getId(), resourceVertexId);
                    notYetExist = false;
                    if (Flag.REMOVE_MYSELF.t().equals(flag)) {
                        // remove node with its whole subtree and do NOT recover this node after (do NOT set its transaction end to the new revision number)
                        graphOperation.setSubtreeTransactionEnd(context.getServerDbTransaction(), child, parentVertexId, latestCommittedRevision);
                    } else if (Flag.REMOVE.t().equals(flag)) {
                        // remove node with its whole subtree but recover this node after (set its transaction end to the new revision number)
                        graphOperation.setSubtreeTransactionEnd(context.getServerDbTransaction(), child, parentVertexId, latestCommittedRevision);
                        revisionUtils.setVertexTransactionEnd(context.getServerDbTransaction(), child, newEndRevision);
                    } else {
                        revisionUtils.setVertexTransactionEnd(context.getServerDbTransaction(), child, newEndRevision);
                    }
                }
            }
            if (notYetExist) {
                Node newNode;
                Long resourceParentId = context.getMapNodeDbIdResourceDbId().get(parentVertexId);
                if (resourceParentId != null && resourceParentId.equals(resourceVertexId)) {
                    newNode = graphCreation.createNode(context.getServerDbTransaction(), parentVertexId, parentEdgeType.t(),
                            name, nodeType, new RevisionInterval(newRevision, newEndRevision));
                } else {
                    newNode = createNodeResource(context, parentVertexId, resourceVertexId, name, nodeType,
                            new RevisionInterval(newRevision, newEndRevision));
                }
                context.getMapNodeDbIdResourceDbId().put(newNode.getId(), resourceVertexId);
                context.getMapNodeIdToDbId().put(nodeId, newNode.getId());
                return ResultType.NEW_OBJECT;
            } else {
                return ResultType.ALREADY_EXIST;
            }
        } else {
            LOGGER.warn(String.format("Incorrect node record: %s", StringUtils.join(itemParameters, DELIMITER)));
            return ResultType.ERROR;
        }
    }

    /**
     * Creates node with different resource than its parent.
     * @param context Context for merger.
     * @param parentVertexId Parent node id.
     * @param resourceVertexId Resource node id.
     * @param name Name of node.
     * @param nodeType Type of node.
     * @param revisionInterval Revision interval.
     * @return Newly created node.
     */
    protected Node createNodeResource(StoredProcessorContext context, Long parentVertexId, Long resourceVertexId,
                                      String name, String nodeType, RevisionInterval revisionInterval) {
        return graphCreation.createNode(context.getServerDbTransaction(), parentVertexId, resourceVertexId, name,
                nodeType, revisionInterval);
    }

    /**
     * Processes edge.
     *
     * If the edge does not exist it is created:
     *  <ul>
     *      <li>
     *          If a node has node parent but no resource, its resource is inherited,
     *          but the control edge is its {@link MantaRelationshipType#HAS_PARENT} relationship.
     *      </li>
     *      <li>
     *          If a node has no parent node the edge between node and its resource is
     *          created ({@link MantaRelationshipType#HAS_PARENT})and serves as the control edge
     *      </li>
     *      <li>
     *          If a node has both parent node and resource, which is different from its parent,
     *          the edges connecting it to both parent and resources are created
     *      </li>
     *  </ul>
     *
     * If the context does not contain either source or target node in set of previously existing nodes,
     * the edge is deemed as non existent.
     *
     * Special type of edge {@link MantaRelationshipType#PERSPECTIVE} is processed in a way that all
     * edges outgoing edges (of the same type) of the source nodes are retrieved and if any of the target nodes of
     * these edges belong to the same layer as the target node the new edge is not created. If that is not the case,
     * the new edge with special attribute containing layer's name is created as well as node attribute of the source node
     * containing the full vertex path of the target node.
     * <br>
     * If the edge exists its revision is updated.
     * <br>
     * Context updates:
     * <ul>
     *     <li>
     *          Mapping of a edge local identifier to a new {@link EdgeIdentification} is stored.
     *     </li>
     * </ul>
     *
     * @param itemParameters  Edge's merge parameters.
     * @param context  Merger context.
     * @return Result of the merging.
     */
    protected Map<ItemTypes, ResultType> processEdge(String[] itemParameters, StoredProcessorContext context) {
        Map<ItemTypes, ResultType> result = new LinkedHashMap<>();
        if (itemParameters.length == ItemTypes.EDGE.getSize()) {
            String edgeId = itemParameters[EdgeFormat.ID.getIndex()];

            Double newRevision = context.getRevision();
            Double newEndRevision = StoredRevisionUtils.getMaxMinorRevisionNumber(newRevision);
            Double latestCommittedRevision = context.getLatestCommittedRevision();

            Long nodeSourceId = context.getMapNodeIdToDbId().get(itemParameters[EdgeFormat.SOURCE.getIndex()]);
            if (nodeSourceId == null) {
                LOGGER.warn(String.format("For edge %s does not exist source vertex.",
                        StringUtils.join(itemParameters, DELIMITER)));
                result.put(ItemTypes.EDGE, ResultType.ERROR);
                return result;
            }

            Long nodeTargetId = context.getMapNodeIdToDbId().get(itemParameters[EdgeFormat.TARGET.getIndex()]);
            if (nodeTargetId == null) {
                LOGGER.warn(String.format("For edge %s does not exist target vertex.",
                        StringUtils.join(itemParameters, DELIMITER)));
                result.put(ItemTypes.EDGE, ResultType.ERROR);
                return result;
            }

            MantaRelationshipType type = MantaRelationshipType.parseFromGraphType(itemParameters[EdgeFormat.TYPE.getIndex()]);
            if (type == null) {
                LOGGER.warn(String.format("Unknown edge type found in: %s", StringUtils.join(itemParameters, DELIMITER)));
                result.put(ItemTypes.EDGE, ResultType.ERROR);
                return result;
            }
            boolean notExistYet = true;
            if (context.getNodesExistedBefore().contains(nodeSourceId) && context.getNodesExistedBefore().contains(nodeTargetId)) {
                List<Relationship> existedEdges = graphOperation.getRelationships(context.getServerDbTransaction(), nodeSourceId,
                        nodeTargetId, type.t(), latestCommittedRevision);
                if (!existedEdges.isEmpty()) {
                    Relationship relationship = existedEdges.get(0);
                    notExistYet = false;
                    revisionUtils.setEdgeTransactionEnd(context.getServerDbTransaction(), relationship, newEndRevision);
                    context.mapEdgeId(edgeId, new EdgeIdentification(relationship.getStartNodeId(),
                            relationship.getEndNodeId(), relationship.getId(),
                            relationship.getType().toString()));
                }
            }

            /* edge does not exit yet -> create
               If the edge is AGGREGATE type, check, if there are any other AGGREGATE edges from this node,
               to the same layer. If it is the case, do not create the edge and log error.
               If it is the first AGGREGATE edge, create it and also add edge attribute with the name of the layer it leads to.
             */
            Node layer = null;
            if (notExistYet) {
                if (type == MantaRelationshipType.PERSPECTIVE) {
                    List<Relationship> aggregatedEdges = graphOperation.getOutgoingRelationships(context.getServerDbTransaction(),
                            nodeSourceId, type.t(), latestCommittedRevision);
                    layer = graphOperation.getLayer(context.getServerDbTransaction(), nodeTargetId);
                    /*
                     * Iterate over all PERSPECTIVE edges.
                     * If there is already a perspective edge leading to the same perspective as the new edge, don't create it and log error.
                     */
                    for (Relationship relationship: aggregatedEdges) {
                        if (layer == graphOperation.getLayer(context.getServerDbTransaction(), relationship.getEndNodeId())) {
                            LOGGER.error(String.format("Another aggregated edge %s already leads to perspective %s from node %s and edge %s can't be created.",
                                    relationship.getId(), graphOperation.getName(layer), nodeSourceId, StringUtils.join(itemParameters, DELIMITER)));
                            result.put(ItemTypes.EDGE, ResultType.ERROR);
                            return result;
                        }
                    }
                }
                RevisionInterval revisionInterval = new RevisionInterval(newRevision, newEndRevision);
                if (type == MantaRelationshipType.PERSPECTIVE) {
                    createPerspectiveEdge(context, layer, edgeId, nodeSourceId, nodeTargetId, type, revisionInterval);
                    result.put(ItemTypes.NODE_ATTRIBUTE, ResultType.NEW_OBJECT);
                    result.put(ItemTypes.EDGE_ATTRIBUTE, ResultType.NEW_OBJECT);
                } else {
                    createEdge(context, edgeId, nodeSourceId, nodeTargetId, type, revisionInterval);
                }
                result.put(ItemTypes.EDGE, ResultType.NEW_OBJECT);
                return result;
            } else {
                result.put(ItemTypes.EDGE, ResultType.ALREADY_EXIST);
                return result;
            }
        } else {
            LOGGER.warn(String.format("Incorrect edge record: %s", StringUtils.join(itemParameters, DELIMITER)));
            result.put(ItemTypes.EDGE, ResultType.ERROR);
            return result;
        }
    }

    /**
     * Creates a new edge between two nodes.
     * @param context Contextual object.
     * @param edgeId Local identifier of the edge.
     * @param nodeSourceId Database identifier of the source node.
     * @param nodeTargetId Database identifier of the target node.
     * @param type Type of the edge.
     * @param revisionInterval Revision of the edge.
     * @return Created relationship.
     */
    protected Relationship createEdge(StoredProcessorContext context, String edgeId,
                                      Long nodeSourceId, Long nodeTargetId, MantaRelationshipType type,
                                      RevisionInterval revisionInterval) {
        Relationship newRelationship = graphCreation.createRelationship(context.getServerDbTransaction(), nodeSourceId,
                nodeTargetId, type, Map.of(), false, revisionInterval);
        context.mapEdgeId(edgeId, new EdgeIdentification(
                newRelationship.getStartNodeId(),
                newRelationship.getEndNodeId(), newRelationship.getId(),
                newRelationship.getType().toString()));
        return newRelationship;
    }

    /**
     * Creates perspective edge.
     *
     * @param context Contextual object.
     * @param layer Layer node.
     * @param edgeId Local identifier of the edge.
     * @param nodeSourceId Database identifier of the source node.
     * @param nodeTargetId Database identifier of the target node.
     * @param type Type of the edge.
     * @param revisionInterval Revision of the edge.
     */
    protected void createPerspectiveEdge(StoredProcessorContext context, Node layer, String edgeId,
                                         Long nodeSourceId, Long nodeTargetId, MantaRelationshipType type,
                                         RevisionInterval revisionInterval) {
        Relationship newRelationship = createEdge(context, edgeId, nodeSourceId, nodeTargetId, type, revisionInterval);
        String layerName = graphOperation.getName(layer);
        graphCreation.createNodeAttribute(context.getServerDbTransaction(), nodeSourceId, layerName + " perspective",
                graphOperation.getVertexPathString(context.getServerDbTransaction(), nodeTargetId), revisionInterval);
        graphCreation.addRelationshipProperty(context.getServerDbTransaction(), newRelationship, EdgeProperty.LAYER.t(), layerName);
    }

    /**
     * Processes edge attribute.
     *
     *  <ul>
     *      <li>
     *          If the target edge does not contain given attribute as a property, new property is added.
     *      </li>
     *      <li>
     *          If the target edge contains given attribute with a different value as a property
     *          and was created within the merged revision, the property is overridden.
     *      </li>
     *      <li>
     *          If the edge contains given attribute with a different value as a property,
     *          but was created in the previous revision, a new edge is created with copied properties,
     *          and the old edge has its revision closed.
     *      </li>
     *  </ul>
     *
     * @param itemParameters  Edge attribute's merge parameters.
     * @param context  Merger context.
     * @return Result of the merging.
     */
    protected ResultType processEdgeAttribute(String[] itemParameters, StoredProcessorContext context) {
        if (itemParameters.length == ItemTypes.EDGE_ATTRIBUTE.getSize()) {
            String objectId = itemParameters[AttributeFormat.OBJECT_ID.getIndex()];
            String rawKey = itemParameters[AttributeFormat.KEY.getIndex()];
            String value = itemParameters[AttributeFormat.VALUE.getIndex()];

            Double newRevision = context.getRevision();
            Double newEndRevision = StoredRevisionUtils.getMaxMinorRevisionNumber(newRevision);
            Double latestCommittedRevision = context.getLatestCommittedRevision();

            EdgeIdentification edgeId = context.getEdgeDbId(objectId);
            if (edgeId == null) {
                LOGGER.warn(String.format("Edge for edge attribute does not exist: %s", objectId));
                return ResultType.ERROR;
            }

            Relationship relationship = graphOperation.getRelationShip(context.getServerDbTransaction(), edgeId);

            if (relationship == null) {
                LOGGER.error(String.format("For target of edge attribute is saved incorrect id: %s."
                        + " Graph DB changes ID of edge when its attribute is created and / or modified,"
                        + " so please check the edge IDs mapping is updated in such cases.", edgeId));
                return ResultType.ERROR;
            }

            final String key;
            // atribut se nesmí jmenovat label, tím by se změnil charakter hrany
            if (LABEL_KEY_WORD.equalsIgnoreCase(rawKey)) {
                key = "_" + rawKey;
                LOGGER.info(String.format("Edge attribute with name %s and value %s changed to %s.", LABEL_KEY_WORD, value, key));
            } else {
                key = rawKey;
            }
            Object actualPropertyValue;
            try {
                actualPropertyValue = relationship.getProperty(key);
            } catch (NotFoundException e) {
                actualPropertyValue = null;
            }
            if (actualPropertyValue == null) {
                graphCreation.addRelationshipProperty(context.getServerDbTransaction(), relationship, key, value);
                return ResultType.NEW_OBJECT;
            } else if (!actualPropertyValue.toString().equals(value)) {
                RevisionInterval currentEdgeInterval = revisionUtils.getRevisionInterval(relationship);
                if (currentEdgeInterval.getStart() < newRevision) {
                    // current edge is from the previous revision (latest committed revision)
                    // a new edge has to be created in the new revision
                    revisionUtils.setEdgeTransactionEnd(context.getServerDbTransaction(), relationship, latestCommittedRevision);
                    Map<String, Object> attributes = new HashMap<>(relationship.getAllProperties());
                    attributes.remove(EdgeProperty.TRAN_START.t());
                    attributes.remove(EdgeProperty.TRAN_END.t());
                    attributes.put(key, value);
                    copyEdge(context, relationship, new RevisionInterval(newRevision, newEndRevision), attributes, objectId);
                } else {
                    //created in this revision
                    graphCreation.addRelationshipProperty(context.getServerDbTransaction(), relationship, key, value);
                }
                return ResultType.NEW_OBJECT;
            } else {
                return ResultType.ALREADY_EXIST;
            }
        } else {
            LOGGER.warn(String.format("Incorrect edge attr record: %s", StringUtils.join(itemParameters, DELIMITER)));
            return ResultType.ERROR;
        }
    }

    /**
     * Processes node attribute.
     *
     * Node attribute is inserted as a new node connected to the specific node by
     * a {@link MantaRelationshipType#HAS_ATTRIBUTE} edge.
     * <br>
     * If a given node attribute does not exist for specific node it is created.
     *
     * There are two special types of node attributes:
     * <ul>
     *     <li>
     *         <b>mapsTo</b> In case of this type, a new edge has to be created to connect the specific node and
     *         the mapped node which is retrieved by the input node attribute value, which is a path from resource
     *         to the node.
     *     </li>
     *     <li>
     *         <b>sourceLocation</b> The value of the attribute is the identifier of
     *         a source code which was merged previously and is contained within context.
     *     </li>
     * </ul>
     *
     * If a given node attributes already exists for the specific node,
     * the revision interval of its control edge is updated.
     *
     * @param itemParameters  Node attribute's merge parameters.
     * @param context  Merger context.
     * @return Result of the merging.
     */
    @SuppressWarnings("unchecked")
    protected MergerProcessorResult processNodeAttribute(String[] itemParameters, StoredProcessorContext context) {
        if (itemParameters.length == ItemTypes.NODE_ATTRIBUTE.getSize()) {
            String objectId = itemParameters[AttributeFormat.OBJECT_ID.getIndex()];
            String key = itemParameters[AttributeFormat.KEY.getIndex()];
            Object value;

            Double newRevision = context.getRevision();
            Double newEndRevision = StoredRevisionUtils.getMaxMinorRevisionNumber(newRevision);
            Double latestCommittedRevision = context.getLatestCommittedRevision();
            try {
                value = Base64AttributeCodingHelper.decodeAttribute(itemParameters[AttributeFormat.VALUE.getIndex()]);
            } catch (IOException e) {
                LOGGER.warn(String.format("Error decoding binary attribute %s for id: %s. I/O error occurred", key,
                        objectId), e);
                return new MergerProcessorResult(ResultType.ERROR, ItemTypes.NODE_ATTRIBUTE);
            } catch (ClassNotFoundException e) {
                LOGGER.warn(String.format("Error decoding binary attribute %s for id: %s. Class not found", key,
                        objectId), e);
                return new MergerProcessorResult(ResultType.ERROR, ItemTypes.NODE_ATTRIBUTE);
            }

            Long nodeVertexId = context.getMapNodeIdToDbId().get(objectId);
            if (nodeVertexId == null) {
                LOGGER.warn(String.format("Object for node attribute does not exist: %s", objectId));
                return new MergerProcessorResult(ResultType.ERROR, ItemTypes.NODE_ATTRIBUTE);
            }
            if (MAPS_TO.equals(key)) {
                // Zpracovavany uzel (zdrojovy) mapuje jiny uzel (cilovy), odkazovany hodnotou atributu "mapsTo"
                // Hodnota je ocekavana jako seznam trojic (cesta k uzlu, typ uzlu, zdroj)
                if (!(value instanceof List<?>)) {
                    LOGGER.warn(String.format("Incorrect type of MAPS_TO atribute value. Expected %s but was %s", List.class,
                            value != null ? value.getClass() : null));
                    return new MergerProcessorResult(ResultType.ERROR, ItemTypes.EDGE);
                }
                ResultType nodeMappingResult = processNodeMapping(context, nodeVertexId, (List<List<String>>) value);
                return new MergerProcessorResult(nodeMappingResult, ItemTypes.EDGE);
            } else if (SOURCE_LOCATION.equals(key)) {
                Object sourceCodeId = context.getMapSourceCodeIdToDbId().get(value.toString());
                if (sourceCodeId == null) {
                    LOGGER.warn(String.format("Source code for node does not exist: %s", objectId));
                    return new MergerProcessorResult(ResultType.ERROR, ItemTypes.NODE_ATTRIBUTE);
                }
                value = sourceCodeId;
            }
            Node attribute = null;
            if (context.getNodesExistedBefore().contains(nodeVertexId)) {
                attribute = graphOperation.getAttribute(context.getServerDbTransaction(), nodeVertexId, key, value,
                        latestCommittedRevision);
            }
            if (attribute != null) {
                revisionUtils.setVertexTransactionEnd(context.getServerDbTransaction(), attribute, newEndRevision);
                return new MergerProcessorResult(ResultType.ALREADY_EXIST, ItemTypes.NODE_ATTRIBUTE);
            } else {
                createNodeAttribute(context, nodeVertexId, key, value, new RevisionInterval(newRevision, newEndRevision));
                return new MergerProcessorResult(ResultType.NEW_OBJECT, ItemTypes.NODE_ATTRIBUTE);
            }
        } else {
            LOGGER.warn(String.format("Incorrect node attr record: %s", StringUtils.join(itemParameters, DELIMITER)));
            return new MergerProcessorResult(ResultType.ERROR, ItemTypes.NODE_ATTRIBUTE);
        }
    }

    /**
     * Processes source code.
     *
     * Source code is inserted into a subtree of {@link MantaNodeLabel#SUPER_ROOT} connected by
     * a {@link MantaRelationshipType#HAS_SOURCE} edge.
     * <br>
     * If a source code does not exist it is created.
     * <br>
     * If a source code node existed, revision interval of its control edge is updated.
     *
     * Context updates:
     * <ul>
     *     <li>
     *          Mapping of a source code local identifier to its UUID is created.
     *     </li>
     * </ul>
     *
     * @param itemParameters  Edge attribute's merge parameters.
     * @param context  Merger context.
     * @return Result of the merging.
     */
    protected ResultType processSourceCode(String[] itemParameters, StoredProcessorContext context) {
        if (!ItemTypes.SOURCE_CODE.checkSize(itemParameters.length)) {
            LOGGER.warn(String.format("Incorrect source code record: %s", StringUtils.join(itemParameters, DELIMITER)));
            return ResultType.ERROR;
        }

        String scTechnology = "";
        String scConnection = "";
        if (itemParameters.length == ItemTypes.SOURCE_CODE.getSize()) {
            scTechnology = itemParameters[SourceCodeFormat.TECHNOLOGY.getIndex()];
            scConnection = itemParameters[SourceCodeFormat.CONNECTION.getIndex()];
        }

        String scId = itemParameters[SourceCodeFormat.ID.getIndex()];
        String scLocalName = itemParameters[SourceCodeFormat.LOCAL_NAME.getIndex()];
        String scHash = itemParameters[SourceCodeFormat.HASH.getIndex()];

        Double newRevision = context.getRevision();
        Double newEndRevision = StoredRevisionUtils.getMaxMinorRevisionNumber(newRevision);
        Double latestCommittedRevision = context.getLatestCommittedRevision();

        Node sourceRoot = sourceRootHandler.getRoot(context.getServerDbTransaction());

        Node sourceCode = graphOperation.getSourceCode(context.getServerDbTransaction(), sourceRoot,
                scLocalName, scTechnology, scConnection, scHash, latestCommittedRevision);
        if (sourceCode != null) {
            revisionUtils.setVertexTransactionEnd(context.getServerDbTransaction(), sourceCode, newEndRevision);
            String scDbId = sourceRootHandler.getSourceNodeId(sourceCode);
            context.getMapSourceCodeIdToDbId().put(scId, scDbId);
            return ResultType.ALREADY_EXIST;
        } else {
            Node sourceCodeVertex = graphCreation.createSourceCode(context.getServerDbTransaction(), sourceRoot,
                    scLocalName, scHash, scTechnology, scConnection, new RevisionInterval(newRevision, newEndRevision));

            String scDbId = sourceRootHandler.getSourceNodeId(sourceCodeVertex);
            context.getMapSourceCodeIdToDbId().put(scId, scDbId);
            context.addRequestedSourceCode(new SourceCodeMapping(scId, scDbId));
            return ResultType.NEW_OBJECT;
        }
    }

    /**
     * Creates {@link MantaRelationshipType#MAPS_TO} edge connecting the source node to the mapped node.
     * @param context Merger context.
     * @param nodeId Source node identifier.
     * @param mappedNodeQualifiedName Path from resource to the target node.
     * @return Result of the merging.
     */
    private ResultType processNodeMapping(StoredProcessorContext context, Long nodeId,
                                          List<List<String>> mappedNodeQualifiedName) {
        Node root = superRootHandler.getRoot(context.getServerDbTransaction());
        Double newRevision = context.getRevision();
        Double newEndRevision = StoredRevisionUtils.getMaxMinorRevisionNumber(newRevision);
        Double latestCommittedRevision = context.getLatestCommittedRevision();

        RevisionInterval newRevisionInterval = new RevisionInterval(newRevision, newRevision);
        // Ziskame mapovany uzel podle kvalifikovaneho jmena
        Node mappedNode = graphOperation.getVertexByQualifiedName(context.getServerDbTransaction(), root, mappedNodeQualifiedName,
                newRevisionInterval);
        if (mappedNode == null) {
            LOGGER.warn(String.format("Unable to find node of resource with qualified name %s.", mappedNodeQualifiedName));
            return ResultType.ERROR;
        }
        // Zjistime, zda jiz existuje mapovaci hrana mezi uzly
        EdgeIdentification edgeId = new EdgeIdentification(nodeId, mappedNode.getId(), null,
                MantaRelationshipType.MAPS_TO.t());
        Relationship existingMapsToEdge = graphOperation.getRelationShip(context.getServerDbTransaction(), edgeId,
                new RevisionInterval(latestCommittedRevision, newRevision));
        if (existingMapsToEdge != null) {
            revisionUtils.setEdgeTransactionEnd(context.getServerDbTransaction(), existingMapsToEdge, newEndRevision);
            return ResultType.ALREADY_EXIST;
        } else {
            // Pokud ne, vytvorime ji
            addMapsToNodeAttribute(context, nodeId, mappedNode.getId(), new RevisionInterval(newRevision, newEndRevision));
            return ResultType.NEW_OBJECT;
        }
    }

    /**
     * Creates relationship between node and target node by maps to attribute
     * @param context Context object.
     * @param nodeId Source node id.
     * @param mappedId Mapped node id.
     * @param newRevisionInterval Revision interval.
     */
    protected void addMapsToNodeAttribute(StoredProcessorContext context, Long nodeId, Long mappedId, RevisionInterval newRevisionInterval) {
        graphCreation.createRelationship(context.getServerDbTransaction(), nodeId, mappedId, MantaRelationshipType.MAPS_TO,
                Map.of(), false, newRevisionInterval);
    }

    /**
     * Adds new attribute.
     * @param context Context object.
     * @param nodeId Node id.
     * @param key Attribute key.
     * @param value Attribute value.
     * @param newRevisionInterval Revision interval.
     */
    protected void createNodeAttribute(StoredProcessorContext context, Long nodeId, String key, Object value, RevisionInterval newRevisionInterval) {
        graphCreation.createNodeAttribute(context.getServerDbTransaction(), nodeId, key, value,
                newRevisionInterval);
    }

    /**
     * Copies edge.
     * @param context Contextual object.
     * @param relationship Edge to be copied
     * @param revisionInterval Revision interval
     * @param attributes Map of attributes
     * @param relationshipId Local edge id
     */
    public void copyEdge(StoredProcessorContext context, Relationship relationship,
                         RevisionInterval revisionInterval, Map<String, Object> attributes, String relationshipId) {
        Relationship newRelationship = graphCreation.copyRelationshipWithNewProperties(context.getServerDbTransaction(),
                relationship, attributes, revisionInterval);

        context.mapEdgeId(relationshipId, new EdgeIdentification(
                newRelationship.getStartNodeId(),
                newRelationship.getEndNodeId(), newRelationship.getId(),
                newRelationship.getType().toString()));
    }

    /**
     * Creates node-resource relationship.
     * @param query Query containing ids of node and resource.
     * @param context Context for merging.
     */
    public void processNodeResourceRelationship(DelayedNodeResourceEdgeQuery query, StoredProcessorContext context) {
        Double newRevision = context.getRevision();
        Double newEndRevision = StoredRevisionUtils.getMaxMinorRevisionNumber(newRevision);
        graphCreation.createRelationship(context.getServerDbTransaction(),
                context.getMapNodeIdToDbId().get(query.getNodeId()),
                context.getMapResourceIdToDbId().get(query.getResourceNodeId()),
                MantaRelationshipType.HAS_RESOURCE,  Map.of(), false,
                new RevisionInterval(newRevision, newEndRevision));
    }

    /**
     * Creates new edge.
     * @param query Delayed edge query.
     * @param context Contextual object.
     */
    public void mergeNewEdge(DelayedEdgeQuery query, StoredProcessorContext context) {
        Relationship newRelationship = graphCreation.createRelationship(context.getServerDbTransaction(), query.getSourceNodeId(),
                query.getTargetNodeId(), query.getEdgeType(), query.getProperties(), false, query.getRevisionInterval());
        context.mapEdgeId(query.getLocalId(), new EdgeIdentification(
                newRelationship.getStartNodeId(),
                newRelationship.getEndNodeId(), newRelationship.getId(),
                newRelationship.getType().toString()));
    }

    /**
     * Creates new perspective edge.
     * @param query Delayed perspective edge query containing edge query and node attribute query.
     * @param context Contextual object.
     */
    public void mergePerspectiveEdge(DelayedPerspectiveEdgeQuery query, StoredProcessorContext context) {
        mergeNewEdge(query, context);
        mergeNewAttribute(query.getNodeAttributeQuery(), context);
    }

    /**
     * Creates new attribute.
     * @param query Delayed node attribute query.
     * @param context Contextual object.
     */
    public void mergeNewAttribute(DelayedNodeAttributeQuery query, StoredProcessorContext context) {
        graphCreation.createNodeAttribute(context.getServerDbTransaction(), query.getNodeId(), query.getAttributeKey(),
                query.getAttributeValue(), query.getRevisionInterval());
    }


}