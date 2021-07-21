package eu.profinit.manta.graphplayground.repository.merger.stored;

import eu.profinit.manta.graphplayground.model.manta.merger.stored.StoredProcessorContext;
import eu.profinit.manta.graphplayground.repository.merger.common.CommonMergerProcessorTest;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphCreation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredGraphOperation;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSourceRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.connector.StoredSuperRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.processor.StoredMergerProcessor;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.revision.StoredRevisionUtils;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * Tests for standard merger.
 *
 * @author tpolacok
 */
public class StoredMergerProcessorTest extends CommonMergerProcessorTest {

    @Override
    protected void beforeEachInit(Double mergedRevision, Double latestCommittedRevision) {
        testGraphUtil = new StoredTestGraphUtil(driver, neo4j.defaultDatabaseService());
        // Init merger processor
        StoredRevisionUtils revisionUtils = new StoredRevisionUtils();
        StoredGraphOperation graphOperation = new StoredGraphOperation(revisionUtils);
        StoredGraphCreation graphCreation = new StoredGraphCreation(graphOperation);
        context = new StoredProcessorContext(mergedRevision, latestCommittedRevision);
        initialProcessor = new StoredMergerProcessor(
                new StoredSuperRootHandler(), new StoredSourceRootHandler(graphOperation), new StoredRevisionRootHandler(),
                graphOperation, graphCreation, revisionUtils, LOGGER
        );
        testedProcessor = new StoredMergerProcessor(
                new StoredSuperRootHandler(), new StoredSourceRootHandler(graphOperation), new StoredRevisionRootHandler(),
                graphOperation, graphCreation, revisionUtils, LOGGER
        );
    }

    /**
     * Dummy logger instance.
     */
    static public final Log LOGGER = new Log() {
        @Override
        public boolean isDebugEnabled() {
            return false;
        }

        @Nonnull
        @Override
        public Logger debugLogger() {
            return null;
        }

        @Override
        public void debug(@Nonnull String s) {

        }

        @Override
        public void debug(@Nonnull String s, @Nonnull Throwable throwable) {

        }

        @Override
        public void debug(@Nonnull String s, @Nullable Object... objects) {

        }

        @Nonnull
        @Override
        public Logger infoLogger() {
            return null;
        }

        @Override
        public void info(@Nonnull String s) {

        }

        @Override
        public void info(@Nonnull String s, @Nonnull Throwable throwable) {

        }

        @Override
        public void info(@Nonnull String s, @Nullable Object... objects) {

        }

        @Nonnull
        @Override
        public Logger warnLogger() {
            return null;
        }

        @Override
        public void warn(@Nonnull String s) {

        }

        @Override
        public void warn(@Nonnull String s, @Nonnull Throwable throwable) {

        }

        @Override
        public void warn(@Nonnull String s, @Nullable Object... objects) {

        }

        @Nonnull
        @Override
        public Logger errorLogger() {
            return null;
        }

        @Override
        public void error(@Nonnull String s) {

        }

        @Override
        public void error(@Nonnull String s, @Nonnull Throwable throwable) {

        }

        @Override
        public void error(@Nonnull String s, @Nullable Object... objects) {

        }

        @Override
        public void bulk(@Nonnull Consumer<Log> consumer) {

        }
    };
}
