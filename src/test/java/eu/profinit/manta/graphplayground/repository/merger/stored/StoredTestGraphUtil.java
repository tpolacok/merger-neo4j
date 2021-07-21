package eu.profinit.manta.graphplayground.repository.merger.stored;


import eu.profinit.manta.graphplayground.model.manta.merger.AbstractProcessorContext;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.merger.common.TestGraphUtil;
import eu.profinit.manta.graphplayground.repository.merger.parallel.query.CommonMergeQuery;
import eu.profinit.manta.graphplayground.repository.merger.processor.MergerProcessor;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;
import org.neo4j.driver.Driver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.List;

public class StoredTestGraphUtil extends TestGraphUtil {

    private GraphDatabaseService graphDatabaseService;

    public StoredTestGraphUtil(Driver driver, GraphDatabaseService graphDatabaseService) {
        super(driver);
        this.graphDatabaseService = graphDatabaseService;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<MergerProcessorResult> merge(MergerProcessor processor, AbstractProcessorContext context,
                                             String[] object) {
        StoredProcessorContext standardProcessorContext = (StoredProcessorContext) context;
        Transaction tx = graphDatabaseService.beginTx();
        standardProcessorContext.setServerDbTransaction(tx);
        List<MergerProcessorResult> result = processor.mergeObject(object, context);
        tx.commit();
        return result;
    }

    public List<MergerProcessorResult> merge(StoredMergerProcessor processor, StoredProcessorContext context,
                                             CommonMergeQuery query) {
        Transaction tx = graphDatabaseService.beginTx();
        context.setServerDbTransaction(tx);
        List<MergerProcessorResult> result = query.evaluate(processor, context);
        tx.commit();
        return result;
    }


}
