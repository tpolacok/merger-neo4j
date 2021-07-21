package eu.profinit.manta.graphplayground.repository.merger.connector;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import eu.profinit.manta.graphplayground.model.manta.merger.MergerType;
import eu.profinit.manta.graphplayground.model.manta.merger.MergingGroup;
import eu.profinit.manta.graphplayground.model.manta.merger.stored.MergerOutput;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.exceptions.InvalidArgumentException;
import org.springframework.stereotype.Repository;

import eu.profinit.manta.graphplayground.repository.GraphSessionRepository;
import eu.profinit.manta.graphplayground.repository.merger.client.ClientMerger;


@Repository
public final class MergerRepository {

    /**
     * Database session repository.
     */
    private GraphSessionRepository<Session, Transaction> sessionRepository;

    private ClientMerger clientMerger;

    /**
     * @param sessionRepository Database session repository.
     */
    public MergerRepository(GraphSessionRepository<Session, Transaction> sessionRepository, ClientMerger clientMerger) {
        this.sessionRepository = sessionRepository;
        this.clientMerger = clientMerger;
    }

    /**
     * Further processes merge objects in the database - either client-side or server-side.
     * @param mergingGroup Merging group - type of merger and revision.
     * @param mergeObjects List of groups of merged objects.
     * @return Merger output.
     */
    public MergerOutput merge(MergingGroup mergingGroup, List<List<List<String>>> mergeObjects) {
        MergerType mergerType = mergingGroup.getMergerType();
        Double revision = mergingGroup.getRevision();
        if (mergerType == MergerType.CLIENT) {
            return clientMerger.processStandardMerge(mergeObjects.stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList()), revision);
        } else {
            return sessionRepository.getSession().writeTransaction(tx -> {
                switch (mergerType) {
                    case SERVER_SEQUENTIAL:
                        return GraphCreation.storedSequentialMerge(tx, mergeObjects, revision);
                    case SERVER_PARALLEL_A:
                    case SERVER_PARALLEL_B:
                        return GraphCreation.storedParallelMerge(tx, mergeObjects, revision,4, mergerType);
                    default:
                        throw new InvalidArgumentException(String.format("Unknown parallel type %s", mergerType));
                }
            });
        }
    }
}
