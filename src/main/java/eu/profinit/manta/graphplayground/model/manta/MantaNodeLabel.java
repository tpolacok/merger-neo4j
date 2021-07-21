package eu.profinit.manta.graphplayground.model.manta;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Direction;

/**
 * Typ vrcholu v Manta Repostiory DB.
 * @author tfechtner
 *
 */
public enum MantaNodeLabel {
    /** Extra node for indexing .*/
    GENERAL_NODE(null, null, null),

    /** Super root databáze, v korektní db musí být právě jeden.*/
    SUPER_ROOT(null, null, "SuperRoot"),
    /** Revision root databáze, v korektní db musí být právě jeden.*/
    REVISION_ROOT(null, null, "RevisionRoot"),
    /** Resource, vrchol odpovídající resource/technologii. */
    RESOURCE(Direction.OUTGOING, new String[] { DatabaseStructure.MantaRelationshipType.HAS_RESOURCE.t() }, "Resource"),
    /** Standardní uzel odpovídající každému prvku grafu datových toků. */
    NODE(Direction.OUTGOING, new String[] { DatabaseStructure.MantaRelationshipType.HAS_RESOURCE.t(), DatabaseStructure.MantaRelationshipType.HAS_PARENT.t() }, "Node"),
    /** Atribut {@link #NODE}. */
    ATTRIBUTE(Direction.INCOMING, new String[] { DatabaseStructure.MantaRelationshipType.HAS_ATTRIBUTE.t() }, "Attribute"),
    /** Uzel reprezentujici revizi grafu. */
    REVISION_NODE(Direction.INCOMING, new String[] { DatabaseStructure.MantaRelationshipType.HAS_REVISION.t() }, "RevisionNode"),
    /** Source root databáze, v korektní db musí být právě jeden.*/
    SOURCE_ROOT(null, null, "SourceRoot"),
    /** Uzel reprezentujici source code file. */
    SOURCE_NODE(Direction.INCOMING, new String[] { DatabaseStructure.MantaRelationshipType.HAS_SOURCE.t()}, "SourceNode"),
    /** Uzel reprezentujici vrstvu datoveho toku. */
    LAYER(Direction.INCOMING, new String[] {DatabaseStructure.MantaRelationshipType.IN_LAYER.t()}, "Layer");

    /**
     * Mapping of String representation to {@link MantaNodeLabel}
     */
    public static Map<String, MantaNodeLabel> MANTA_NODE_LABEL_CONVERSION_MAP;

    /**
     * Initialization of {@link conversionMap}
     * Map {@link vertexLabel} to {@link MantaNodeLabel}
     */
    static {
        MANTA_NODE_LABEL_CONVERSION_MAP = new HashMap<>();
        for (MantaNodeLabel label: MantaNodeLabel.values()) {
            MANTA_NODE_LABEL_CONVERSION_MAP.put(label.getVertexLabel(), label);
        }
    }

    /** Směr kontrolních hran pro tento typ .*/
    private final Direction controlEdgeDirection;
    /** Labely kontrolních hran pro tento typ. */
    private final String[] controlEdgeLabels;

    /**
     * Label of bertex
     */
    private String vertexLabel;

    /**
     * @param controlEdgeDirection směr kontrolních hran pro tento typ
     * @param controlEdgeLabels labely kontrolních hran pro tento typ
     * @param vertexLabel label of vertex
     */
    MantaNodeLabel(Direction controlEdgeDirection, String[] controlEdgeLabels, String vertexLabel) {
        this.controlEdgeDirection = controlEdgeDirection;
        this.controlEdgeLabels = controlEdgeLabels;
        this.vertexLabel = vertexLabel;
    }

    /**
     * @return Label of the vertex type.
     */
    public String getVertexLabel() {
        return vertexLabel;
    }

    /**
     * @return směr kontrolních hran pro tento typ
     */
    public Direction getControlEdgeDirection() {
        return controlEdgeDirection;
    }

    /**
     * @return labely kontrolních hran pro tento typ
     */
    public String[] getControlEdgeLabels() {
        return controlEdgeLabels;
    }

}