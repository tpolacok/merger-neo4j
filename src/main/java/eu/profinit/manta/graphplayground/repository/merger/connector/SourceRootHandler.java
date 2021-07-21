package eu.profinit.manta.graphplayground.repository.merger.connector;

import java.io.File;
import java.io.IOException;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import eu.profinit.manta.graphplayground.model.manta.util.DriverMantaNodeLabelConverter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.Validate;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

/**
 * Třída pro správu source root uzlu databáze.
 * @author tfechtner
 *
 */
@Repository
public class SourceRootHandler extends AbstractRootHandler {
    /** SL4J logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRootHandler.class);

    /** Adresář obsahující soubory reprezentující zdrojový kód. */
    private File sourceCodeDir = new File("sourceCode");

    /**
     * Graph operation instance.
     */
    private GraphOperation graphOperation;

    public SourceRootHandler(GraphOperation graphOperation) {
        this.graphOperation = graphOperation;
    }

    @Override
    public DatabaseStructure.NodeProperty getNodeProperty() {
        return DatabaseStructure.NodeProperty.SOURCE_ROOT;
    }

    @Override
    public MantaNodeLabel getMantaNodeLabel() {
        return MantaNodeLabel.SOURCE_ROOT;
    }

    /**
     * @return Adresář obsahující soubory reprezentující zdrojový kód.
     */
    public File getSourceCodeDir() {
        return sourceCodeDir;
    }

    /**
     * @param sourceCodeDir Adresář obsahující soubory reprezentující zdrojový kód.
     */
    public void setSourceCodeDir(File sourceCodeDir) {
        this.sourceCodeDir = sourceCodeDir;
    }

    /**
     * Získá id pro source code soubor reprezentovaný daným vrcholem.
     * @param vertex vrchol, který reprezentuje dotazovaný source code soubor
     * @return id source code souboru
     * @throws IllegalStateException vrchol nemá příslušnou property
     */
    public String getSourceNodeId(Node vertex) {
        Value property = vertex.get(DatabaseStructure.NodeProperty.SOURCE_NODE_ID.t());
        if (property != null) {
            return property.asString();
        } else {
            throw new IllegalStateException("The vertex " + graphOperation.getName(vertex) + " does not have "
                    + DatabaseStructure.NodeProperty.SOURCE_NODE_ID.t() + " property.");
        }
    }

    /**
     * Odstraní vrchol reprezentující source code soubor včetně příslušného souboru.
     * @param vertex vrchol k odstranění
     */
    public void removeSourceCodeNode(Node vertex) {
        Validate.isTrue(DriverMantaNodeLabelConverter.getType(vertex) == MantaNodeLabel.SOURCE_NODE, "The vertex is not source code node.");
        String id = getSourceNodeId(vertex);
        File file = new File(sourceCodeDir, id);
        if (file.exists() && !file.delete()) {
            LOGGER.warn("Cannot delete source code file " + file.getName() + ".");
        }
        //TODO remove
//        vertex.remove();
    }

    /**
     * Smaže všechny source code soubory.
     */
    public void truncateSourceCodeFiles() {
        try {
            if (getSourceCodeDir().exists()) {
                FileUtils.cleanDirectory(getSourceCodeDir());
            }
        } catch (IOException e) {
            LOGGER.error("Cannot clean source code directory", e);
        }

    }
}
