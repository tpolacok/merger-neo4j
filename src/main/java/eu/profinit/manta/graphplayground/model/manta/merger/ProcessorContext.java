package eu.profinit.manta.graphplayground.model.manta.merger;

import java.util.Map;
import java.util.Set;

import eu.profinit.manta.graphplayground.model.manta.core.EdgeIdentification;

/**
 * Processor context interface.
 *
 * Defines mappings between local merged objects and their database counterparts.
 *
 * @author tpolacok
 */
public interface ProcessorContext {

    /**
     * @return Map of local node it to its database id.
     */
    Map<String, Long> getMapNodeIdToDbId();

    /**
     * @return Map of local resource it to its database id.
     */
    Map<String, Long> getMapResourceIdToDbId();
    /**
     * @return Map of local source code id it to its database id.
     */
    Map<String, String> getMapSourceCodeIdToDbId();

    /**
     * @return Specific data holder for merger.
     */
    SpecificDataHolder getSpecificDataHolder();

    /**
     * @return Revision number for current merge.
     */
    double getRevision();

    /**
     * @return Latest committed revision number.
     */
    Double getLatestCommittedRevision();

    /**
     * @param sourceCodeMapping Adds source code mapping.
     */
    void addRequestedSourceCode(SourceCodeMapping sourceCodeMapping);

    /**
     * @return Set of requested source code mappings.
     */
    Set<SourceCodeMapping> getRequestedSourceCodes();

    /**
     * @return Map of local layer it to its database id.
     */
    Map<String, String[]> getMapLayerIdToLayer();

    /**
     * Adds mapping of MANTA edge ID to database edge ID and vice versa.
     * @param mantaId MANTA edge ID
     */
    void mapEdgeId(String mantaId, EdgeIdentification dbId);

    /**
     * Returns edge database ID corresponding to the MANTA ID.
     * @param edgeMantaId The MANTA edge ID
     * @return Edge database ID corresponding to the MANTA ID.
     */
    EdgeIdentification getEdgeDbId(String edgeMantaId);

    /**
     * @return Return mappng of node db id to its resource db id.
     */
    Map<Long, Long> getMapNodeDbIdResourceDbId();

    /**
     * @return Set of nodes which existed before being merged.
     */
    Set<Long> getNodesExistedBefore();
}
