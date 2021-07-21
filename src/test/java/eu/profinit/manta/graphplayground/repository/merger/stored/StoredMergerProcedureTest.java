package eu.profinit.manta.graphplayground.repository.merger.stored;

import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats;
import eu.profinit.manta.graphplayground.model.manta.core.DataflowObjectFormats.ItemTypes;
import eu.profinit.manta.graphplayground.model.manta.merger.MergerType;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.MergerOutput;
import eu.profinit.manta.graphplayground.repository.GraphSessionRepository;
import eu.profinit.manta.graphplayground.repository.merger.Neo4jTestGraphSessionRepository;
import eu.profinit.manta.graphplayground.repository.merger.connector.GraphCreation;
import eu.profinit.manta.graphplayground.repository.merger.connector.GraphOperation;
import eu.profinit.manta.graphplayground.repository.merger.connector.SourceRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.connector.SuperRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.revision.RevisionRootHandler;
import eu.profinit.manta.graphplayground.repository.stored.merger.StoredMerger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.*;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Tests for stored merger procedure.
 *
 * @author tpolacok
 */
public class StoredMergerProcedureTest {

    private static final Config driverConfig = Config.builder().withoutEncryption().build();

    private static Driver driver;

    private static Neo4j embeddedDatabaseServer;

    private static GraphSessionRepository<Session, Transaction> graphSessionRepository;

    private Double mergedRevision;

    private String MERGE_FILE = "/merge/db2/db2_merge_3.csv";

    @BeforeAll
    static void initializeNeo4j() {
        embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .withProcedure(StoredMerger.class)
                .build();

        driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
        graphSessionRepository = new Neo4jTestGraphSessionRepository(driver);
    }

    private void initMerger() {
        Transaction tx = graphSessionRepository.getActiveTransaction();
        new SuperRootHandler().ensureRootExistance(tx);
        new SourceRootHandler(new GraphOperation()).ensureRootExistance(tx);
        RevisionRootHandler rh = new RevisionRootHandler();
        rh.ensureRootExistance(tx);
        // Technical revision
        Double rev = rh.createMajorRevision(tx);
        rh.commitRevision(tx, rev);
        mergedRevision = rh.createMajorRevision(tx);
        tx.commit();
    }

    @BeforeEach
    private void beforeEach() {
        graphSessionRepository.getSession().run("MATCH (n) DETACH DELETE n");
        initMerger();
    }


    @Test
    public void testMergeEmpty() {
        Transaction tx = graphSessionRepository.getActiveTransaction();

        MergerOutput output = GraphCreation.storedSequentialMerge(
                tx,
                Stream.generate(ArrayList<List<String>>::new)
                        .limit(ItemTypes.values().length)
                        .collect(Collectors.toList()),
                mergedRevision
        );

        tx.commit();

        assertThat( "Processed objects number",output.getProcessedObjects(), is(0L));
        assertThat( "Processed unknown objects number",output.getUnknownTypes(), is(0L));
    }

    @Test
    public void testMergeSequential() {
        Transaction tx = graphSessionRepository.getActiveTransaction();

        List<List<List<String>>> mergeObjects = getMergeObjectsFromFile(MERGE_FILE);
        MergerOutput output = GraphCreation.storedSequentialMerge(
                tx,
                mergeObjects,
                mergedRevision
        );
        tx.commit();

        evaluateMergeOutput(output);
    }

    @Test
    public void testMergeParallelTransactionRetry_2thread() {
        Transaction tx = graphSessionRepository.getActiveTransaction();

        List<List<List<String>>> mergeObjects = getMergeObjectsFromFile(MERGE_FILE);
        MergerOutput output = GraphCreation.storedParallelMerge(
                tx,
                mergeObjects,
                mergedRevision,
                2,
                MergerType.SERVER_PARALLEL_A
        );

        tx.commit();

        evaluateMergeOutput(output);
    }

    @Test
    public void testMergeParallelTransactionRetry_3thread() {
        Transaction tx = graphSessionRepository.getActiveTransaction();

        List<List<List<String>>> mergeObjects = getMergeObjectsFromFile(MERGE_FILE);
        MergerOutput output = GraphCreation.storedParallelMerge(
                tx,
                mergeObjects,
                mergedRevision,
                3,
                MergerType.SERVER_PARALLEL_A
        );

        tx.commit();

        evaluateMergeOutput(output);
    }

    @Test
    public void testMergeParallelDeadlockFree_2thread() {
        Transaction tx = graphSessionRepository.getActiveTransaction();
        List<List<List<String>>> mergeObjects = getMergeObjectsFromFile(MERGE_FILE);
        MergerOutput output = GraphCreation.storedParallelMerge(
                tx,
                mergeObjects,
                mergedRevision,
                2,
                MergerType.SERVER_PARALLEL_B
        );

        tx.commit();

        evaluateMergeOutput(output);
    }

    @Test
    public void testMergeParallelDeadlockFree_3thread() {
        Transaction tx = graphSessionRepository.getActiveTransaction();

        List<List<List<String>>> mergeObjects = getMergeObjectsFromFile(MERGE_FILE);
        MergerOutput output = GraphCreation.storedParallelMerge(
                tx,
                mergeObjects,
                mergedRevision,
                3,
                MergerType.SERVER_PARALLEL_B
        );

        tx.commit();

        evaluateMergeOutput(output);
    }

    private void evaluateMergeOutput(MergerOutput output) {
        assertThat( "Processed objects number",output.getProcessedObjects(), is(975L));
        assertThat( "Processed unknown objects number",output.getUnknownTypes(), is(0L));

        assertThat( "Processed source code number",output.getNewObjects().get("source_code"), is(3));
        assertThat( "Processed layers number",output.getNewObjects().get("layer"), is(2));
        assertThat( "Processed resource number",output.getNewObjects().get("resource"), is(2));
        assertThat( "Processed node number",output.getNewObjects().get("node"), is(154));
        assertThat( "Processed node attribute number",output.getNewObjects().get("node_attribute"), is(123));
        assertThat( "Processed edge number",output.getNewObjects().get("edge"), is(690));
        assertThat( "Processed edge attribute number",output.getNewObjects().get("edge_attribute"), is(1));
    }

    private List<List<List<String>>> getMergeObjectsFromFile(String file) {
        try (InputStream inputStream = this.getClass().getResourceAsStream(file)) {
            return getMergeObjectsGroups(new InputStreamReader(inputStream));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return List.of();
    }

    private List<List<List<String>>> getMergeObjectsGroups(InputStreamReader is)
            throws IOException {
        List<List<List<String>>> mergedObjectsGroups =
                Stream.generate(() -> (List<List<String>>) new ArrayList<List<String>>())
                        .limit(ItemTypes.values().length)
                        .collect(Collectors.toList());
        BufferedReader reader = new BufferedReader(is);
        while(reader.ready()) {
            String line = reader.readLine();
            List<String> items = Arrays.asList(line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1));
            items = items.stream().map(element -> element.substring(1, element.length() - 1)).collect(Collectors.toList());
            DataflowObjectFormats.ItemTypes itemType = DataflowObjectFormats.ItemTypes.parseString(items.get(0));
            int listIndex = itemType.getInputFilePosition();
            mergedObjectsGroups.get(listIndex).add(items);
        }
        return mergedObjectsGroups;
    }
}
