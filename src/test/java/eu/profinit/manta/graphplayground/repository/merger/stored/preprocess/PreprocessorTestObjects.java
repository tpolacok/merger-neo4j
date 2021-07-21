package eu.profinit.manta.graphplayground.repository.merger.stored.preprocess;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.MantaRelationshipType;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedEdgeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.DelayedNodeAttributeQuery;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.EdgeRawMergeQuery;

import java.util.List;

/**
 * Test objects for preprocessor tests.
 *
 * @author tpolacok
 */
public class PreprocessorTestObjects {

    /**
     * List of resources to preprocess.
     */
    public static List<List<String>> RESOURCES = List.of(
            List.of("resource","1","DB2","DB2","DB2 Database","0"),
            List.of("resource","2","DB2 Scripts","DB2 Scripts","DB2 Database","0")
    );

    /**
     * List of nodes to preprocess.
     */
    public static List<List<String>> NODES = List.of(
            List.of("node","3","","IISDOCKER\\DB2","Server","1"),
            List.of("node","4","","IADB","Database","1"),
            List.of("node","5","4","ADMINISTRATOR","Schema","2")
    );

    /**
     * List of node attributes to preprocess.
     */
    public static List<List<String>> NODE_ATTRIBUTES = List.of(
            List.of("node_attribute","3","OBJECT_SOURCE_TYPE","DATABASE"),
            List.of("node_attribute","4","ORDER","1"),
            List.of("node_attribute","4","mapsTo","1"),
            List.of("node_attribute","6","DUMMY","VAL")
    );

    /**
     * List of delayed node attributes to preprocess.
     */
    public static List<CommonMergeQuery> NODE_ATTRIBUTES_DELAYED = List.of(
            new DelayedNodeAttributeQuery(1L, "a", "b", null),
            new DelayedNodeAttributeQuery(2L, "a2", "b2", null),
            new DelayedNodeAttributeQuery(2L, "a21", "b21", null),
            new DelayedNodeAttributeQuery(3L, "a3", "b3", null),
            new DelayedNodeAttributeQuery(4L, "a4", "b4", null),
            new DelayedNodeAttributeQuery(5L, "a4", "b4", null)
    );

    /**
     * List of edge attributes to preprocess.
     */
    public static List<List<String>> EDGE_ATTRIBUTES = List.of(
            List.of("edge_attribute","3","OBJECT_SOURCE_TYPE","DATABASE"),
            List.of("edge_attribute","4","ORDER","1"),
            List.of("edge_attribute","5","DUMMY","1")
    );

    /**
     * List of edges to preprocess.
     */
    public static List<List<String>> EDGES = List.of(
            List.of("edge","3","1","2", "DIRECT"),
            List.of("edge","4","2","3", "DIRECT"),
            List.of("edge","5","3","4", "DIRECT"),
            List.of("edge","6", "4", "5","PERSPECTIVE")
    );

    /**
     * List of delayed edges to preprocess.
     */
    public static List<CommonMergeQuery> EDGES_DELAYED = List.of(
            new DelayedEdgeQuery("1", 1L, 2L,
                    MantaRelationshipType.DIRECT, null, null),
            new DelayedEdgeQuery("2", 2L, 3L,
                    MantaRelationshipType.DIRECT, null, null),
            new DelayedEdgeQuery("3", 4L, 5L,
                    MantaRelationshipType.DIRECT, null, null),
            new DelayedEdgeQuery("4", 2L, 6L,
                    MantaRelationshipType.DIRECT, null, null),
            new DelayedEdgeQuery("5", 5L, 7L,
                    MantaRelationshipType.DIRECT, null, null),
            new DelayedEdgeQuery("6", 7L, 5L,
                    MantaRelationshipType.DIRECT, null, null)
    );

    /**
     * List of raw merge edges to preprocess.
     */
    public static List<CommonMergeQuery> RAW_MERGE_EDGES = List.of(
            new EdgeRawMergeQuery(List.of("edge","3","1","2", "DIRECT")),
            new EdgeRawMergeQuery(List.of("edge","4","2","3", "DIRECT")),
            new EdgeRawMergeQuery(List.of("edge","5","4","5", "DIRECT")),
            new EdgeRawMergeQuery(List.of("edge","6", "2", "6","DIRECT")),
            new EdgeRawMergeQuery(List.of("edge","7", "5", "7","DIRECT")),
            new EdgeRawMergeQuery(List.of("edge","8", "7", "5","DIRECT"))
    );

}
