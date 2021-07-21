package eu.profinit.manta.graphplayground.repository.merger.client;


import eu.profinit.manta.graphplayground.model.manta.merger.AbstractProcessorContext;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerProcessorResult;
import eu.profinit.manta.graphplayground.model.manta.merger.StandardProcessorContext;
import eu.profinit.manta.graphplayground.repository.merger.common.TestGraphUtil;
import eu.profinit.manta.graphplayground.repository.merger.processor.MergerProcessor;
import org.neo4j.driver.Driver;

import java.util.List;

public class StandardTestGraphUtil extends TestGraphUtil {

    public StandardTestGraphUtil(Driver driver) {
        super(driver);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<MergerProcessorResult> merge(MergerProcessor processor, AbstractProcessorContext context,
                                             String[] object) {
        StandardProcessorContext standardProcessorContext = (StandardProcessorContext) context;
        return driver.session().writeTransaction(tx -> {
            standardProcessorContext.setDbTransaction(tx);
            return processor.mergeObject(object, context);
        });
    }


}
