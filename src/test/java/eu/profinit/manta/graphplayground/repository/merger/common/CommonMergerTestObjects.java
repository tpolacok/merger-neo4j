package eu.profinit.manta.graphplayground.repository.merger.common;

import eu.profinit.manta.graphplayground.model.manta.merger.RevisionInterval;
import eu.profinit.manta.graphplayground.model.manta.util.Base64AttributeCodingHelper;

import java.io.IOException;
import java.util.List;

/**
 * Common objects for merger tests.
 *
 * @author tpolacok
 */
public class CommonMergerTestObjects {

    /**
     * Revision interval.
     */
    public static final RevisionInterval REVISION_INTERVAL = new RevisionInterval(1.0, 1.999999);

    /**
     * Revision interval closed.
     */
    public static final RevisionInterval REVISION_INTERVAL_CLOSED = new RevisionInterval(1.0, 1.0);

    /**
     * Updated revision interval.
     */
    public static final RevisionInterval REVISION_INTERVAL_UPDATED = new RevisionInterval(1.0, 2.999999);

    public static final String[] MERGED_SOURCE_CODE = {
            "source_code","44",
            "c:\\mantaflow\\cli\\temp\\oracle\\manta\\ddl\\BSL\\Functions\\IS_TABLE_EXIST",
            "5d21aba1fac6150170f4c77b757821e7",
            "ORACLE",
            "manta"
    };

    public static final String[] MERGED_LAYER = {
            "layer",
            "0",
            "Physical",
            "Physical"
    };

    public static final String[] MERGED_LAYER_2 = {
            "layer",
            "1",
            "Logical",
            "Logical"
    };

    public static final String[] MERGED_RESOURCE = {
            "resource",
            "2",
            "Oracle DDL",
            "Oracle scripts",
            "Oracle DDL scripts",
            "0"
    };

    public static final String[] MERGED_RESOURCE_2 = {
            "resource",
            "10",
            "Oracle DDL Second",
            "Oracle scripts Second",
            "Oracle DDL scripts",
            "1"
    };

    public static final String[] MERGED_NODE_RESOURCE = {
            "node",
            "3",
            "",
            "manta",
            "Connection",
            "2"
    };

    public static final String[] MERGED_NODE_RESOURCE_2 = {
            "node",
            "11",
            "",
            "manta_logical",
            "Connection_logical",
            "10"
    };

    public static final String[] MERGED_NODE_PARENT = {
            "node",
            "4",
            "3",
            "INFA",
            "Directory",
            "2"
    };

    public static final String[] MERGED_NODE_PARENT_2 = {
            "node",
            "40",
            "3",
            "Data",
            "File",
            "2"
    };

    public static final String[] MERGED_NODE_REMOVE_MYSELF = {
            "node",
            "3",
            "",
            "manta",
            "Connection",
            "2",
            "removeMyself"
    };

    public static final String[] MERGED_NODE_REMOVE = {
            "node",
            "3",
            "",
            "manta",
            "Connection",
            "2",
            "remove"
    };

    public static final String[] MERGED_NODE_PARENT_DIFFERENT_RESOURCE = {
            "node",
            "4",
            "3",
            "INFA",
            "Directory",
            "10"
    };

    public static final String[] MERGED_NODE_ATTRIBUTE = {
            "node_attribute",
            "3",
            "OBJECT_SOURCE_TYPE",
            "DICTIONARY"
    };

    public static final String[] MERGED_NODE_ATTRIBUTE_SOURCE_CODE = {
            "node_attribute",
            "3",
            "sourceLocation",
            "44"
    };

    public static String[] MERGED_NODE_ATTRIBUTE_MAPS_TO;

    public static String[] MERGED_NODE_ATTRIBUTE_MAPS_TO_MISSING;

    public static final String[] MERGED_EDGE = {
            "edge",
            "0",
            "3",
            "4",
            "DIRECT",
            "2"
    };

    public static final String[] MERGED_PERSPECTIVE_EDGE = {
            "edge",
            "0",
            "3",
            "11",
            "PERSPECTIVE",
            "2"
    };

    public static final String[] MERGED_PERSPECTIVE_EDGE_INVALID = {
            "edge",
            "0",
            "3",
            "4",
            "PERSPECTIVE",
            "2"
    };

    public static final String[] MERGED_EDGE_ATTRIBUTE = {
            "edge_attribute",
            "0",
            "EDGE_PROP",
            "edgVal"
    };

    public static final String[] MERGED_EDGE_ATTRIBUTE_2 = {
            "edge_attribute",
            "0",
            "EDGE_PROP",
            "edgVal2"
    };

    static {
        try {
            MERGED_NODE_ATTRIBUTE_MAPS_TO = new String[]{
                    "node_attribute",
                    "4",
                    "mapsTo",
                    Base64AttributeCodingHelper.encodeAttribute(
                            List.of(
                                    List.of("Oracle DDL", "Oracle", "Oracle DDL"),
                                    List.of("manta", "connection", "Oracle DDL")
                            ))
            };
            MERGED_NODE_ATTRIBUTE_MAPS_TO_MISSING = new String[]{
                    "node_attribute",
                    "4",
                    "mapsTo",
                    Base64AttributeCodingHelper.encodeAttribute(
                            List.of(
                                    List.of("Oracle DDL", "Oracle", "Oracle DDL2"),
                                    List.of("manta", "connection", "Oracle DDL")
                            ))
            };
        } catch (IOException e) {
            MERGED_NODE_ATTRIBUTE_MAPS_TO = new String[]{};
            e.printStackTrace();
        }
    }
}
