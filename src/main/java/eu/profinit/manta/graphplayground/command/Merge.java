package eu.profinit.manta.graphplayground.command;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import eu.profinit.manta.graphplayground.model.manta.merger.MergerType;
import eu.profinit.manta.graphplayground.repository.merger.connector.GraphOperation;
import eu.profinit.manta.graphplayground.service.MergerService;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import eu.profinit.manta.graphplayground.repository.GraphSessionRepository;
import eu.profinit.manta.graphplayground.repository.merger.connector.SourceRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.connector.SuperRootHandler;
import eu.profinit.manta.graphplayground.repository.merger.revision.RevisionRootHandler;

@Service
public class Merge extends AbstractCommand {
    private final static Logger LOGGER = LoggerFactory.getLogger(Merge.class);

    /**
     * Merger instance.
     */
    private MergerService mergerService;

    /**
     * Database session repository.
     */
    private GraphSessionRepository<Session, Transaction> sessionRepository;

    public Merge(MergerService mergerService, GraphSessionRepository<Session, Transaction> sessionRepository) {
        this.mergerService = mergerService;
        this.sessionRepository = sessionRepository;
    }

    private static final String ARG_MERGE = "merge";
    private static final String ARG_MERGE_TYPE = "type";

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder()
                .longOpt(ARG_MERGE)
                .desc("CSV File with merges")
                .hasArgs()
                .required()
                .build()
        );
        options.addOption(Option.builder()
                .longOpt(ARG_MERGE_TYPE)
                .desc("Type of merge")
                .hasArgs()
                .build()
        );

        return options;
    }

    @Override
    public void run(Map<String, Option> args) throws Exception {
        clean();
        init();
        Double rev = newRevision();
        String mergeFolder = args.get(ARG_MERGE).getValue();
        MergerType mergerType = MergerType.valueOf(args.get(ARG_MERGE_TYPE).getValue(MergerType.CLIENT.toString()));
        Long start = System.currentTimeMillis();
        Set<String> fileList = new HashSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(mergeFolder))) {
            for (Path path : stream) {
                if (!Files.isDirectory(path)) {
                    fileList.add(path.getFileName().toString());
                }
            }
        }
        for (String fileName: fileList) {
            try (InputStream inputStream = new FileInputStream(mergeFolder + "/" + fileName)) {
                mergerService.process(inputStream, null, rev, mergerType);
            }
        }

        commitRevision(rev);
        Long end = System.currentTimeMillis();
        LOGGER.info("Merge time {}", end - start);
    }

    private void clean(){
        sessionRepository.getSession().writeTransaction(tx -> tx.run( "MATCH (n) DETACH DELETE n"));
    }

    /**
     * Helper method to commit revision
     */
    private void commitRevision(Double rev) {
        sessionRepository.getSession().writeTransaction(tx -> {
            new RevisionRootHandler().commitRevision(tx, rev);
            return null;
        });
    }

    private Double newRevision() {
        return sessionRepository.getSession().writeTransaction(tx -> new RevisionRootHandler().createMajorRevision(tx));
    }

    private void init() {
        sessionRepository.getSession().writeTransaction(tx -> {
            new SuperRootHandler().ensureRootExistance(tx);
            new SourceRootHandler(new GraphOperation()).ensureRootExistance(tx);
            RevisionRootHandler rh = new RevisionRootHandler();
            rh.ensureRootExistance(tx);
            // Technical revision
            Double rev = rh.createMajorRevision(tx);
            rh.commitRevision(tx, rev);
            return null;
        });
    }
}