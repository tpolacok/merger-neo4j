package eu.profinit.manta.graphplayground.repository.merger.parallel.query;

public interface EdgeQuery extends CommonMergeQuery{

    /**
     * @return Identification of the edge.
     */
    String identify();
}
