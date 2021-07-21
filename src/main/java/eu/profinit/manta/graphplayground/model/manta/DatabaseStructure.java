package eu.profinit.manta.graphplayground.model.manta;

import java.util.HashSet;
import java.util.Set;

/**
 * Konstaty používané pro strukturní objekty v databázi.
 * @author tfechtner
 *
 */
public class DatabaseStructure {
    /**
     * Možné property uzlů.
     * @author tfechtner
     * @author jsykora
     */
    public enum NodeProperty {
        /** Nazev vrstvy. */
        LAYER_NAME("layerName"),
        /** Typ vrstvy. */
        LAYER_TYPE("layerType"),

        /** Název resource. */
        RESOURCE_NAME("resourceName"),
        /** Typ resource. */
        RESOURCE_TYPE("resourceType"),
        /** Popis resource. */
        RESOURCE_DESCRIPTION("resourceDesc"),

        /** Typ uzlu, myslí se uzel dataflow grafu, nikoliv Vertex databáze. */
        NODE_TYPE("nodeType"),
        /** Jméno uzlu, myslí se uzel dataflow grafu, nikoliv Vertex databáze.*/
        NODE_NAME("nodeName"),

        /** Jméno atributu uzlu. */
        ATTRIBUTE_NAME("attributeName"),
        /** Hodnota atributu uzlu. */
        ATTRIBUTE_VALUE("attributeValue"),

        /** Atribut označující super root celého grafu db. */
        SUPER_ROOT("superRoot"),

        /** Atribut označující kořenový uzel pro revize. */
        REVISION_ROOT("revisionRoot"),
        /** Revision root attribute holding the latest committed revision number */
        LATEST_COMMITTED_REVISION("latestCommittedRevision"),
        /** Revision root attribute holding the latest uncommitted revision number */
        LATEST_UNCOMMITTED_REVISION("latestUncommittedRevision"),

        /** Atribut označující číslo revize revizního uzlu. */
        REVISION_NODE_REVISION("revisionNumber"),
        /** Atribut označující zda byla revize kompletně dokončena. */
        REVISION_NODE_COMMITTED("revisionCommitted"),
        /** Atribut označující čas dokončení zamergování nové revize do grafu. */
        REVISION_NODE_COMMIT_TIME("revisionEnd"),
        /**
         * Revision node property holding the previous revision number (immediately preceding revision number).
         * Pozor - uzel teto revize nemusi byt v DB, pokud probehl prune!
         */
        REVISION_NODE_PREVIOUS_REVISION("previousRevisionNumber"),
        /** Revision node property holding the next revision number (immediately following revision number) */
        REVISION_NODE_NEXT_REVISION("nextRevisionNumber"),

        /** Atribut definující typ uzlu. */
        VERTEX_TYPE("vertexType"),

        /** Root node attribute of the source code file content. */
        SOURCE_ROOT("sourceRoot"),
        /** Local name attribute of the source code file content. */
        SOURCE_NODE_LOCAL("sourceNodeLocal"),
        /** Unique name attribute of the source code file content. */
        SOURCE_NODE_ID("sourceNodeId"),
        /** Hash attribute of the source code file content. */
        SOURCE_NODE_HASH("sourceNodeHash"),
        /** Technology attribute of the source code file content. */
        SOURCE_NODE_TECHNOLOGY("sourceNodeTechnology"),
        /** Connection attribute of the source code file content. */
        SOURCE_NODE_CONNECTION("sourceNodeConnection");

        private final String text;

        /**
         * @param text textová hodnota pro databázi
         */
        NodeProperty(String text) {
            this.text = text;
        }

        /**
         * @return textová hodnota pro databázi
         */
        public String t() {
            return text;
        }
    }

    /**
     * Možné label/typy hran.
     * @author tfechtner
     *
     */
    public enum MantaRelationshipType {
        /** Zdroj hrany má jako rodiče cíl hrany. */
        HAS_PARENT("hasParent", "parent"),
        /** Zdroj hrany má jako resource cíl hrany. */
        HAS_RESOURCE("hasResource", "resource"),
        /** Zdroj hrany má cíl hrany jako revizi. */
        HAS_REVISION("hasRevision", "revision"),
        /** Zdroj hrany má cíl hrany jako atribut. */
        HAS_ATTRIBUTE("hasAttribute", "attribute"),
        /** Ze zdroje hrany vede přímy datový tok do jejího cíle.*/
        DIRECT("directFlow", "DIRECT"),
        /** Ze zdroje hrany vede nepřímý datový tok do jejího cíle.*/
        FILTER("filterFlow", "FILTER"),
        /** Zdroj hrany má cíl hrany jako source code. */
        HAS_SOURCE("hasSource", "source"),
        /** Zdroj hrany je ve vrstve reprezentovane cilem. */
        IN_LAYER("inLayer", "layer"),
        /** Zdroj hrany se mapuje na cil. */
        MAPS_TO("mapsTo", "MAPS_TO"),
        /** Target of the edge is alternative parent of the source for aggregatre lineage in different perspective*/
        PERSPECTIVE("perspective","PERSPECTIVE");

        private final String text;

        private final String graphRepr;

        /**
         * @param text  textová hodnota pro databázi
         * @param graphRepr reprezentace pro dataflow graf
         */
        MantaRelationshipType(String text, String graphRepr) {
            this.text = text;
            this.graphRepr = graphRepr;
        }

        /**
         * @return textová hodnota pro databázi
         */
        public String t() {
            return text;
        }

        /**
         * @return reprezentace pro dataflow graf
         */
        public String getGraphRepr() {
            return graphRepr;
        }

        /**
         * Factory pro parsování názvu hrany v graphu na db ekvivalenci.
         * @param input graph reprezentace pro parsování
         * @return enum instance, nebo null
         */
        public static MantaRelationshipType parseFromGraphType(String input) {
            for (MantaRelationshipType item : values()) {
                if (item.graphRepr.equals(input)) {
                    return item;
                }
            }

            return null;
        }

        /**
         * Factory pro parsování db názvu hrany.
         * @param input db reprezentace pro parsování
         * @return enum instance, nebo null
         */
        public static MantaRelationshipType parseFromDbType(String input) {
            for (MantaRelationshipType item : values()) {
                if (item.text.equals(input)) {
                    return item;
                }
            }

            return null;
        }
    }

    /**
     * Možné property hran.
     * @author tfechtner
     *
     */
    public enum EdgeProperty {
        /**
         * Property používaná pro parent hranu pro rychlejší vyhledávání.
         * Obsahuje jméno potomka, tedy jméno uzlu u zdrojového konce.
         */
        CHILD_NAME("childName"),
        /**
         * Property používaná pro direct a filter hranu pro rychlejší vyhledávání.
         * Obsahuje id cíle.
         */
        TARGET_ID("targetId"),

        /** Property určující počáteční platnou revizi cílového uzlu.*/
        TRAN_START("tranStart"),
        /** Property určující poslední platnou revizi cílového uzlu.*/
        TRAN_END("tranEnd"),

        /**
         * Property používaná pro source hranu pro rychlejší vyhledávání.
         * Obsahuje lokální název souboru.
         */
        SOURCE_LOCAL_NAME("sourceLocalName"),
        /**
         * Poperty urcujici, zda je hrana interpolovana.
         * Pokud neni uvedena, bere se, jako by byla {@code false}.
         */
        INTERPOLATED("interpolated"),
        /**
         * Property used for aggregated lineage.
         * Layer/perspective to which the edge leads.
         */
        LAYER("layer");

        private final String text;

        /**
         * @param text textová hodnota pro databázi
         */
        EdgeProperty(String text) {
            this.text = text;
        }

        /**
         * @return textová hodnota pro databázi
         */
        public String t() {
            return text;
        }

        /**
         * Sesbírá názvy všech property v tomto enumu.
         * @return množina názvů všech property
         */
        public static Set<String> collectPropertyNames() {
            Set<String> names = new HashSet<>();
            for (EdgeProperty property : EdgeProperty.values()) {
                names.add(property.t());
            }
            return names;
        }
    }
}
