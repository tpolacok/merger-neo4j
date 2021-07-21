package eu.profinit.manta.graphplayground.repository.merger.parallel.preprocessor.phaser;

import java.util.ArrayList;
import java.util.List;

import eu.profinit.manta.graphplayground.model.manta.merger.parallel.phase.MergePhase;

/**
 * Phaser for queue processing.
 *
 * Used for different phase processing.
 *
 * Examples of phases:
 * <ul>
 *     <Li>
 *         Node attributes - one phase is reading / non-blocking phases, where non-blocking queries
 *         are resolved and rest of queries are delayed for further preprocessing followed by next
 *         phase - blocking phase.
 *     </Li>
 *     <Li>
 *          Edges - same as attributes - first is reading / non-blocking phase followed after blocking phase
 *      </Li>
 * </ul>
 *
 * @author tpolacok.
 */
public class QueuePhaser {

    /**
     * List of lists of parallel queues.
     */
    private List<MergePhase> phaseList = new ArrayList<>();

    /**
     * Empty constructor.
     */
    public QueuePhaser() {
    }

    /**
     * @param initialPhase Initial phase - list of queues.
     */
    public QueuePhaser(MergePhase initialPhase) {
        phaseList.add(initialPhase);
    }

    /**
     * @return Get current phase - list of queues.
     */
    public MergePhase getCurrentPhase() {
        return phaseList.get(phaseList.size() - 1);
    }

    /**
     * @param nextPhase Sets new phase.
     */
    public void nextPhase(MergePhase nextPhase) {
        phaseList.add(nextPhase);
    }

    /**
     * @return Get current phase size.
     */
    public int phaseSize() {
        if (phaseList.isEmpty()) return 0;
        return phaseList.get(phaseList.size() - 1).size();
    }
}
