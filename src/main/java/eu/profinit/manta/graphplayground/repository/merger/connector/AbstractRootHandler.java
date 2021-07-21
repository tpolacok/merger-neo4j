package eu.profinit.manta.graphplayground.repository.merger.connector;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure;
import eu.profinit.manta.graphplayground.model.manta.MantaNodeLabel;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Společný předek pro root handlery.
 * @author pholecek
 *
 */
public abstract class AbstractRootHandler {
    /** Název modulu, který se používá pro práci s root uzlem. */
    public static final String MODULE_NAME = "mr_basic";
    /** SLF4J logger.*/
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRootHandler.class);

    /**
     * Zajistí, že v databázi je daný root. <br>
     * Pokud ještě neexistoval, tak ho vytvoří.
     * @param tx R/W transakce pro případné vytvoření
     * @return root v databázi, nikdy null
     */
    public Node ensureRootExistance(Transaction tx) {
        Node root = tryGetRoot(tx);

        if (root == null) {
            // neexistuje -> vytvořit a uložit si jeho id
            root = tx.run( String.format("CREATE (v:%s) RETURN v", getMantaNodeLabel().getVertexLabel()))
                    .single()
                    .get("v")
                    .asNode();
            LOGGER.info("Created " + getNodeProperty().t() + " root with id " + root.id() + ".");
        }
        return root;
    }

    /**
     * Vrátí root databáze.
     * Přičemž si ho pro optimalizaci vnitřně udržuje.
     * Pokud ještě uzel neexistuje, vyhodí výjimku.
     * @param tx transakce do databáze
     * @return root databáze
     */
    public Node getRoot(Transaction tx) {
        Node root = tryGetRoot(tx);
        if (root != null) {
            return root;
        } else {
            throw new IllegalStateException("Root does not exist, incorrect db.");
        }
    }

    /**
     * Pokusí se načíst root.
     * @param tx čtecí transakce použitá pro získání rootu
     * @return root databáze nebo null, pokud neexistuje
     */
    public Node tryGetRoot(Transaction tx) {
        Result rootNode = tx.run(String.format("MATCH (n:%s) RETURN n LIMIT 1", getMantaNodeLabel().getVertexLabel()));
        return rootNode.hasNext() ? rootNode.single().get(0).asNode() : null;
    }

    /**
     * @return node property definující root uzel, který tento handler drží
     */
    public abstract DatabaseStructure.NodeProperty getNodeProperty();

    /**
     * @return typ root uzlu, který tento handler drží
     */
    public abstract MantaNodeLabel getMantaNodeLabel();

}
