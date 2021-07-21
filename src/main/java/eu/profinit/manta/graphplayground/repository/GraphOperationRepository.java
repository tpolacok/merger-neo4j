package eu.profinit.manta.graphplayground.repository;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import eu.profinit.manta.graphplayground.model.manta.RevisionInterval;
import org.neo4j.graphdb.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Atomic graph operations originally from Manta's GraphOperations
 *
 * @author dbucek
 */
public abstract class GraphOperationRepository<N, R> {
    private final static Logger LOGGER = LoggerFactory.getLogger(GraphOperationRepository.class);

    /*
     * Abstract graph operation
     */
    public abstract N getNodeByProperty(String propertyName, Object propertyValue);

    public abstract N getNodeById(long id);

    public abstract List<N> getNodesByOriginalIds(List<Long> potentialStartNodeIds);

    public abstract N getParentNode(N node);

    public abstract List<N> getAdjacentNodes(N node, Direction direction, String... relationshipType);

    /**
     * Get nodes together with their
     * <p>
     * Key: Relationship
     * Value: Opposite node
     *
     * @param node             Node from opposite we are looking for
     * @param direction        Outgoing or incoming
     * @param revisionInterval Revision interval
     * @return Map relation as key and opposite node as value
     */
    public abstract Map<R, N> getOppositeDirectFlowNode(N node, Direction direction, RevisionInterval revisionInterval);

    public abstract N getDirectResource(N actualNode);

    public abstract Set<N> getDirectChildren(N node);

    public abstract boolean isLocal(N node);

    public abstract long getNodeId(N node);

    public abstract Set<N> getAllNodes();

    public abstract String getNodeProperty(N node, String propertyName);

    public abstract String getRelationshipProperty(R relationship, String propertyName);

    public abstract String getRelationshipType(R relationship);

    public abstract MantaNodeLabel getNodeTypeByLabel(N node);

    public abstract List<Long> callRoutineDataFlow(List<N> startNodes, Direction direction, RevisionInterval revisionInterval);

    /*
     *
     * Method with no direct access to the database
     *
     */
    public boolean isElementDisabled(N node) {
        String nodeTechnology = getNodeTechnology(node);
        String nodeType = getNodeTypeByProperty(node);
        String elementString = nodeTechnology + ":" + nodeType;

        return "PostgreSQL:Server".equals(elementString) || "PostgreSQL scripts:Block".equals(elementString);
    }

    public String getNodeTypeByProperty(N node) {
        MantaNodeLabel type = getNodeTypeByLabel(node);

        //TODO: this method is possible simplified - if we unified 'type' property across nodes
        switch (type) {
            case RESOURCE:
                return getNodeProperty(node, "resourceType");
            case NODE:
                return getNodeProperty(node, "nodeType");
            case ATTRIBUTE:
                return getNodeProperty(node, "layerType");
            default:
                throw new RuntimeException("This node doesn't have type property.");
        }

    }

    public String getNodeTechnology(N node) {
        N resourceNode = getResourceNode(node);

        return getNodeTypeByProperty(resourceNode);
    }

    public N getResourceNode(N node) {
        Objects.requireNonNull(node);

        MantaNodeLabel type = getNodeTypeByLabel(node);
        switch (type) {
            case SUPER_ROOT:
                throw new IllegalArgumentException("The argument node was super root.");
            case RESOURCE:
                return node;
            case NODE:
                N actualNode = node;
                N resourceNode;

                do {
                    resourceNode = getDirectResource(actualNode);
                    if (resourceNode != null) {
                        break;
                    }
                    actualNode = getParentNode(actualNode);
                } while (actualNode != null);

                if (resourceNode != null) {
                    return resourceNode;
                } else {
                    LOGGER.warn("node " + node + " does not have resource.");
                    throw new IllegalStateException("node " + node + " does not have resource.");
                }
//            case ATTRIBUTE:
//                return getResourceNode(getAttributeOwner(node));
            default:
                throw new IllegalArgumentException("node " + node + " has unknwon type " + type + ".");
        }
    }

    /**
     * Vrati vsechny listy dosazitelne ze zadanych uzlu.
     *
     * @param nodes seznam uzlů, pro něž chceme najít listové potomky
     * @return seznam listových potomků, nikdy null
     */
    public Set<N> getStartNodes(Set<N> nodes) {
        Set<N> startListNodeSet = new HashSet<>();

        // získat všechny listové startovní uzly
        for (N node : nodes) {
            startListNodeSet.addAll(getAggregatedLists(node));
        }

        return startListNodeSet;
    }

    private Collection<? extends N> getAggregatedLists(N node) {
        if (node == null) {
            throw new IllegalArgumentException("The argument vertex was null.");
        }
//        if (revInterval == null) {
//            throw new IllegalArgumentException("The argument revInterval must not be null.");
//        }

        List<N> listList = new ArrayList<>();

        /* If the node does not exist in the revision, throw an exception */
//        if (!RevisionUtils.isVertexInRevisionInterval(node, revInterval)) {
//            throw new IllegalStateException(
//                    "The argument node does not exist in the desired revision " + revInterval.toString() + ".");
//        }

        Deque<N> nodeDeque = new LinkedList<>();
        nodeDeque.add(node);

        while (nodeDeque.size() > 0) {
            N actualNode = nodeDeque.pollLast();

            List<N> children = getAdjacentNodes(actualNode, Direction.INCOMING, DatabaseStructure.MantaRelationshipType.HAS_PARENT.t(), DatabaseStructure.MantaRelationshipType.PERSPECTIVE.t());
            if (children.size() == 0) {
                listList.add(actualNode);
            } else {
                nodeDeque.addAll(children);
            }
        }

        return listList;
    }

    public List<N> getAllParents(N node) {
        List<N> parentList = new ArrayList<>();

        if (node != null) {
            N actualNode = node;
            while ((actualNode = getParentNode(actualNode)) != null) {
                // Add to the list if not filtered
                if (!isElementDisabled(actualNode)) {
                    parentList.add(actualNode);
                }
            }
        }

        return parentList;
    }
}
