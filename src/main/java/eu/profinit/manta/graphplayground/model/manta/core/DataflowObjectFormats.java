package eu.profinit.manta.graphplayground.model.manta.core;

import java.util.Arrays;
import java.util.Map;

/**
 * Třída obsahující konstanty definující formát zpráv pro local id merger.
 * @author tfechtner
 *
 */
public final class DataflowObjectFormats {

    private DataflowObjectFormats() {
    }

    /**
     * Typy objektů ve zprávách.
     * @author tfechtner
     *
     */
    public enum ItemTypes {
        /** Vrstva datoveho toku */
        LAYER("layer", 4, 1),
        /** Resource - teradata bteq, teradata ddl, ifpc, ... */
        RESOURCE("resource", 6, 2),
        /** Uzel dataflow grafu. */
        NODE("node", 7, 3),
        /** Hrana v dataflow grafu - direct/flow. */
        EDGE("edge", 6, 5),
        /** Atribut uzlu dataflow grafu. */
        NODE_ATTRIBUTE("node_attribute", 4, 4),
        /** Atribut hrany v dataflow uzlu. */
        EDGE_ATTRIBUTE("edge_attribute", 4, 6),
        /** Source code. */
        SOURCE_CODE("source_code", 6, 0, 4);

        /** Text representation of the type in the message. */
        private final String text;
        /** Number of columns. */
        private final int size;
        /** List of the previous acceptable numbers of columns. */
        private final int[] supportedSizes;

        /**
         * Position in input merge file.
         */
        private final int inputFilePosition;

        /**
         *
         * @param text Textová reprezentace typu ve zprávě.
         * @return odpovídající instance enumu nebo null, pokud taková neexistuje
         */
        public static ItemTypes parseString(String text) {
            for (ItemTypes item : values()) {
                if (item.getText().equals(text)) {
                    return item;
                }
            }

            throw new IllegalArgumentException("Unknown type " + text);
        }

        /**
         * @param text Text representation of the type in the message
         * @param size Number of columns
         * @param inputFilePosition Position in the input file.
         * @param supportedSizes List of the previous acceptable numbers of columns
         */
        ItemTypes(String text, int size, int inputFilePosition, int... supportedSizes) {
            this.text = text;
            this.size = size;
            this.inputFilePosition = inputFilePosition;
            this.supportedSizes = supportedSizes;
        }

        /**
         * @param size number of columns to check
         * @return true if given size equals to the actual size or any of the supported older sizes.
         */
        public boolean checkSize(int size) {
            return this.size == size || Arrays.stream(supportedSizes).anyMatch(supportedSize -> supportedSize == size);
        }

        /**
         * @return Textová reprezentace typu ve zprávě.
         */
        public String getText() {
            return text;
        }

        /**
         * @return Počet sloupců záznamu.
         */
        public int getSize() {
            return size;
        }

        /**
         * @return Input file position.
         */
        public int getInputFilePosition() {
            return inputFilePosition;
        }
    }

    /**
     * Format zprav pro vrstvy.
     *
     * @author onouza
     *
     */
    public enum LayerFormat {
        /** ID vrstvy. */
        ID(1),
        /** Nazev vrstvy. */
        NAME(2),
        /** Typ vrstvy. */
        TYPE(3);

        private final int index;

        /**
         * @param index index ve zprávě
         */
        LayerFormat(int index) {
            this.index = index;
        }

        /**
         * @return index ve zprávě
         */
        public int getIndex() {
            return index;
        }
    }

    /**
     * Formát zpráv pro uzly.
     * @author tfechtner
     *
     */
    public enum NodeFormat {
        /** ID uzlu. */
        ID(1),
        /** Id předka uzlu, prázdný string pokud předka nemá. */
        PARENT_ID(2),
        /** Jméno uzlu.*/
        NAME(3),
        /** Typ uzlu (tabulka, view, sloupec). */
        TYPE(4),
        /** Id resource, do kterého uzlu patří. */
        RESOURCE_ID(5),
        /** Special mark of the node indicating a special operation to be performed on this node. */
        FLAG(6);

        private final int index;

        /**
         * @param index index ve zprávě
         */
        NodeFormat(int index) {
            this.index = index;
        }

        /**
         * @return index ve zprávě
         */
        public int getIndex() {
            return index;
        }
    }

    /**
     * Type of flag sent in the FLAG NodeFormat.
     * Used for additional operation during incremental update.
     *
     * @author jsykora
     */
    public enum Flag {
        /** Remove node and its subtree during incremental update (but recover subtree root node after)*/
        REMOVE("remove"),
        /** Remove node and its subtree during incremental update (do not recover subtree root node after*/
        REMOVE_MYSELF("removeMyself");

        /** Flag in the form of text sent in the CSV */
        private final String text;

        Flag(String text) {
            this.text = text;
        }

        public String t() {
            return text;
        }
    }

    /**
     * Formát zpráv resource.
     * @author tfechtner
     *
     */
    public enum ResourceFormat {
        /** Id resource. */
        ID(1),
        /** Jméno resource. */
        NAME(2),
        /** Typ resource. */
        TYPE(3),
        /** Popis resource. */
        DESCRIPTION(4),
        /** ID vrstvy. */
        LAYER(5);

        private final int index;

        /**
         * @param index index ve zprávě
         */
        ResourceFormat(int index) {
            this.index = index;
        }

        /**
         * @return index ve zprávě
         */
        public int getIndex() {
            return index;
        }
    }

    /**
     * Formát zpráv hran. 
     * @author tfechtner
     *
     */
    public enum EdgeFormat {
        /** Id hrany.*/
        ID(1),
        /** Id zdrojového uzlu hrany. */
        SOURCE(2),
        /** Id koncového uzlu hrany.*/
        TARGET(3),
        /** Typ hrany - direct/flow {@link eu.profinit.manta.dataflow.repository.core.model.DatabaseStructure.EdgeLabel}.*/
        TYPE(4),
        /** Resource of edge. */
        RESOURCE(5);

        private final int index;

        /**
         * @param index index ve zprávě
         */
        EdgeFormat(int index) {
            this.index = index;
        }

        /**
         * @return index ve zprávě
         */
        public int getIndex() {
            return index;
        }
    }

    /**
     * Formát zpráv atributů.
     * @author tfechtner
     *
     */
    public enum AttributeFormat {
        /** Id objektu, ke kterému se atribut vztahuje. */
        OBJECT_ID(1),
        /** Klíč/název atributu. */
        KEY(2),
        /** hodnota atributu. */
        VALUE(3);

        private final int index;

        /**
         * @param index index ve zprávě
         */
        AttributeFormat(int index) {
            this.index = index;
        }

        /**
         * @return index ve zprávě
         */
        public int getIndex() {
            return index;
        }
    }

    /**
     * Formát zpráv pro source code.
     * @author tfechtner
     *
     */
    public enum SourceCodeFormat {
        /** Source code ID. */
        ID(1),
        /** Source code name. */
        LOCAL_NAME(2),
        /** Hash for identifying changes.*/
        HASH(3),
        /** Source code technology.*/
        TECHNOLOGY(4),
        /** SOurce code connection.*/
        CONNECTION(5);

        private final int index;

        /**
         * @param index index ve zprávě
         */
        SourceCodeFormat(int index) {
            this.index = index;
        }

        /**
         * @return index ve zprávě
         */
        public int getIndex() {
            return index;
        }
    }
}
