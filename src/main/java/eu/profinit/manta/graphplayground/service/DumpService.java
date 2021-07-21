package eu.profinit.manta.graphplayground.service;

import eu.profinit.manta.graphplayground.model.ObjectType;
import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import eu.profinit.manta.graphplayground.repository.DumpRepository;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * DumpService for parsing input Dump - zip file
 *
 * @author dbucek
 */
@Service
public class DumpService {
    private final static Logger LOGGER = LoggerFactory.getLogger(DumpService.class);

    private final static String ORIGINAL_ID = "originalId";

    private final DumpRepository dumpRepository;

    public DumpService(DumpRepository dumpRepository) {
        this.dumpRepository = dumpRepository;
    }

    /**
     * Název souboru v zipu, který obsahuje dump.
     */
    private static final String DUMP_ENTRY_NAME = "dump";
    private static final int ITEMS_PER_TRANSACTION_LIMIT = 500_000;

    private final List<Map<String, Object>> revisionRootParameters = new ArrayList<>();
    private final List<Map<String, Object>> layerParameters = new ArrayList<>();
    private final List<Map<String, Object>> resourceParameters = new ArrayList<>();
    private final List<Map<String, Object>> nodeParameters = new ArrayList<>();
    private final List<Map<String, Object>> revisionNodeParameters = new ArrayList<>();
    private final List<Map<String, Object>> sourceRootParameters = new ArrayList<>();
    private final List<Map<String, Object>> sourceNodeParameters = new ArrayList<>();
    private final List<Map<String, Object>> attributeParameters = new ArrayList<>();

    //    private final List<Map<String, Object>> attributes = new ArrayList<>();
    private final List<Map<String, Object>> relationships = new ArrayList<>();

    /**
     *
     */
    public void init() {
        dumpRepository.init();
    }

    /**
     * Import dump
     *
     * @param inputStream Input stream of dump file
     */
    public void importDump(InputStream inputStream) {
        try (ZipArchiveInputStream zipInputStream = new ZipArchiveInputStream(inputStream)) {
            ZipArchiveEntry entry;
            while ((entry = zipInputStream.getNextZipEntry()) != null) {
                // find the non-source data
                if (DUMP_ENTRY_NAME.equals(entry.getName())) {
                    importDumpStart(zipInputStream);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void importDumpStart(ZipArchiveInputStream zipInputStream) throws IOException {
        ObjectInputStream objectInputStream = new ObjectInputStream(zipInputStream);

        // Start processing
        dumpRepository.startProcessing();

        // Only once on beginning of file
        LOGGER.trace("Visit Version");
        // Read a version to get know if the dump is compatible
        String versionOfDump = objectInputStream.readUTF();
        if (!"1.22".equals(versionOfDump)) {
            throw new IllegalArgumentException("Current version '1.22' is not compatible with imported dump version '" + versionOfDump + "'.");
        }

        try {
            int counter = 0;
            boolean verticesPhase = true;

            String objectTypeString;

            // EOF exception will actually breaks the loop
            while (true) {
                objectTypeString = objectInputStream.readUTF();
                if (counter < ITEMS_PER_TRANSACTION_LIMIT) {
                    // Start of each element + must check end of file
                    ObjectType objectType = ObjectType.valueOf(objectTypeString);
                    switch (objectType) {
                        case VERTEX:
                            importVertex(objectInputStream);
                            break;
                        case EDGE:
                            if (verticesPhase) {
                                storeVertices();
                                verticesPhase = false;
                            }

                            importRelationship(objectInputStream);
                            break;
                        default:
                            throw new IllegalStateException("Unknown object type " + objectType.toString() + ".");
                    }
                } else {
                    if (verticesPhase) {
                        storeVertices();
                    } else {
                        storeRelationships();
                    }
                }

                counter++;
            }
        } catch (EOFException e) {
            LOGGER.trace("End of file");
        }

        // Write the rest
        storeRelationships();

        // End processing
        dumpRepository.endProcessing();
    }

    private void importVertex(ObjectInputStream objectInputStream) {
        try {
            String vertexTypeString = objectInputStream.readUTF();
            LOGGER.trace("Vertex type: {}", vertexTypeString);

            MantaNodeLabel mantaNodeLabel = MantaNodeLabel.valueOf(vertexTypeString);
            switch (mantaNodeLabel) {
                case SUPER_ROOT:
                    LOGGER.trace("Add SuperRoot");

                    long superRootOriginalId = objectInputStream.readLong();
                    dumpRepository.storeSuperRoot(superRootOriginalId);
                    break;
                case REVISION_ROOT:
                    LOGGER.trace("Add RevisionRoot");

                    revisionRootParameters.add(Map.of(
                            ORIGINAL_ID, objectInputStream.readLong(),
                            "latestCommittedRevision", objectInputStream.readDouble(),
                            "latestUncommittedRevision", objectInputStream.readDouble()
                    ));
                    break;
                case LAYER:
                    LOGGER.trace("Add Layer");

                    layerParameters.add(Map.of(
                            ORIGINAL_ID, objectInputStream.readLong(),
                            "layerName", objectInputStream.readUTF(),  //TODO: adjust terminology. Suggestion: just name ('layer' is redundant it's already labeled) - neo4j use name as a title of node in viewer
                            "layerType", objectInputStream.readUTF()  //TODO: adjust terminology. Suggestion: just name ('layer' is redundant it's already labeled) - neo4j use name as a title of node in viewer
                    ));
                    break;
                case RESOURCE:
                    LOGGER.trace("Add Resource");

                    resourceParameters.add(Map.of(
                            ORIGINAL_ID, objectInputStream.readLong(),
                            "resourceName", objectInputStream.readUTF(), //TODO: adjust terminology. Suggestion: just name ('resource' is redundant it's already labeled) - neo4j use name as a title of node in viewer
                            "resourceType", objectInputStream.readUTF(), //TODO: adjust terminology. Suggestion: just type ('resource' is redundant it's already labeled)
                            "resourceDesc", objectInputStream.readUTF() //TODO: adjust terminology. Suggestion: just description ('resource' is redundant it's already labeled)
                    ));

                    break;
                case NODE:
                    LOGGER.trace("Add Node");

                    nodeParameters.add(Map.of(
                            ORIGINAL_ID, objectInputStream.readLong(),
                            "nodeName", objectInputStream.readUTF(), //TODO: adjust terminology. Suggestion: just name ('node' is redundant) - neo4j use name as a title of node in viewer
                            "nodeType", objectInputStream.readUTF() //TODO: adjust terminology. Suggestion: just type ('node' is redundant)
                    ));

                    break;
                case ATTRIBUTE:
                    LOGGER.trace("Add Attribute");
                    try {
                        long attributeNodeOriginalId = objectInputStream.readLong();
                        String key = objectInputStream.readUTF();
                        Object value = objectInputStream.readObject(); // Object nebo UTF8???

                        if (value instanceof String) {
                            LOGGER.trace("String");
                        } else {
                            LOGGER.info("Not a string");
                        }

                        attributeParameters.add(Map.of(
                                ORIGINAL_ID, attributeNodeOriginalId,
                                "attributeName", key,
                                "attributeValue", value
                        ));
                    } catch (ClassNotFoundException e) {
                        // Suppress exception during import of some dumps
//                        e.printStackTrace();
                        LOGGER.warn(e.toString());
                    }
                    break;
                case REVISION_NODE:
                    LOGGER.trace("Add Revision");

                    revisionNodeParameters.add(Map.of(
                            ORIGINAL_ID, objectInputStream.readLong(),
                            "revisionNumber", objectInputStream.readDouble(), //TODO: adjust terminology. Suggestion: revision
                            "revisionCommitted", objectInputStream.readBoolean(), //TODO: adjust terminology. Suggestion: isCommitted (shown it is boolean not a number)
                            "commitTimeString", objectInputStream.readUTF(),
                            "previousRevisionNumber", objectInputStream.readDouble(), //TODO: adjust terminology. Suggestion: revisionPrevious
                            "nextRevisionNumber", objectInputStream.readDouble() //TODO: adjust terminology. Suggestion: revisionNext
                    ));
                    break;
                case SOURCE_ROOT:
                    LOGGER.trace("Add SourceRoot");

                    sourceRootParameters.add(Map.of(
                            ORIGINAL_ID, objectInputStream.readLong()
                    ));
                    break;
                case SOURCE_NODE:
                    LOGGER.trace("Add SourceNode");

                    sourceNodeParameters.add(Map.of(
                            ORIGINAL_ID, objectInputStream.readLong(),
                            "sourceNodeLocal", objectInputStream.readUTF(),
                            "sourceNodeHash", objectInputStream.readUTF(),
                            "sourceNodeId", objectInputStream.readUTF()
                    ));
                    break;
                default:
                    throw new IllegalStateException("Unknown vertex type " + mantaNodeLabel.toString() + ".");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void importRelationship(ObjectInputStream objectInputStream) {
        LOGGER.trace("Import Relationship");

        try {
            long startId = objectInputStream.readLong();
            long endId = objectInputStream.readLong();
            String type = objectInputStream.readUTF();

            DatabaseStructure.MantaRelationshipType relationshipType = DatabaseStructure.MantaRelationshipType.parseFromDbType(type);

            double revisionStart = objectInputStream.readDouble();
            double revisionEnd = objectInputStream.readDouble();

            // Read properties of relationship
            Map<String, Object> properties = new HashMap<>();
            long attrCount = objectInputStream.readInt();
            for (int i = 0; i < attrCount; i++) {
                String key = objectInputStream.readUTF();
                Object value = objectInputStream.readObject();
                if (value != null) {
                    properties.put(key, value);
                } else {
                    LOGGER.warn("Deserialized object for property {} was null.", key);
                }
            }

            relationships.add(Map.of(
                    "fromOriginalId", startId,
                    "toOriginalId", endId,
                    "relationshipType", Objects.requireNonNull(relationshipType).t(),
                    "tranStart", revisionStart, //TODO: adjust terminology. Suggestion: revisionStart
                    "tranEnd", revisionEnd,  //TODO: adjust terminology. Suggestion: revisionEnd
                    "properties", properties
            ));

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Store all vertices read in batch
     */
    private void storeVertices() {
        dumpRepository.storeRevisionRoots(revisionRootParameters);
        dumpRepository.storeLayers(layerParameters);
        dumpRepository.storeResources(resourceParameters);
        dumpRepository.storeNodes(nodeParameters);
        dumpRepository.storeAttributes(attributeParameters);
        dumpRepository.storeRevisionNodes(revisionNodeParameters);
        dumpRepository.storeSourceRoots(sourceRootParameters);
        dumpRepository.storeSourceNodes(sourceNodeParameters);

        revisionRootParameters.clear();
        layerParameters.clear();
        resourceParameters.clear();
        nodeParameters.clear();
        revisionNodeParameters.clear();
        sourceRootParameters.clear();
        sourceNodeParameters.clear();
    }

    /**
     * Store all relationships read in batch
     */
    private void storeRelationships() {
        dumpRepository.storeRelationships(relationships);
        relationships.clear();
    }
}
