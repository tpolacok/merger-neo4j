package eu.profinit.manta.graphplayground.model.manta.revision;

import org.neo4j.driver.types.Node;

import eu.profinit.manta.graphplayground.model.manta.DatabaseStructure.NodeProperty;

/**
 * Datová třídá o revizi. <br />
 * Porovnávání probíhá přes číslo revize vzestupně.
 * @author tfechtner
 * @author jsykora
 *
 */
public class RevisionModel implements Comparable<RevisionModel> {
    /** Číslo revize. */
    private final Double revision;
    /**
     * Revision node property holding the previous revision number (immediately preceding revision number).
     * Pozor - uzel teto revize nemusi byt v DB, pokud probehl prune!
     */
    private final Double previousRevision;
    /** Next revision number. */
    private final Double nextRevision;
    //TODO SHOULD BE DATE
    /** Čas commitnutí. */
    private final String commitTime;
    /** True, jestliže je revize commitnutá. */
    private final boolean isCommitted;

    /**
     * @param vertex vertex, pro který se vytváří
     */
    public RevisionModel(Node vertex) {
        revision = vertex.get(NodeProperty.REVISION_NODE_REVISION.t()) != null ?
                vertex.get(NodeProperty.REVISION_NODE_REVISION.t()).asDouble() : null;
        if (revision == null) {
            throw new IllegalStateException("The revision node does not have defined a revision number.");
        }
        
        Double previousRevisionRetrieved = vertex.get(NodeProperty.REVISION_NODE_PREVIOUS_REVISION.t()) != null ?
                vertex.get(NodeProperty.REVISION_NODE_PREVIOUS_REVISION.t()).asDouble() : null;

        if (previousRevisionRetrieved == null || previousRevisionRetrieved == -1.0) {
            previousRevision = null;
        } else {
            previousRevision = previousRevisionRetrieved;
        }
        
        Double nextRevisionRetrieved = vertex.get(NodeProperty.REVISION_NODE_NEXT_REVISION.t()) != null ?
                vertex.get(NodeProperty.REVISION_NODE_NEXT_REVISION.t()).asDouble() : null;
        if (nextRevisionRetrieved == null || nextRevisionRetrieved == -1.0) {
            nextRevision = null;
        } else {
            nextRevision = nextRevisionRetrieved;
        }

        Boolean isCommittedObject = vertex.get(NodeProperty.REVISION_NODE_COMMITTED.t()) != null ?
                vertex.get(NodeProperty.REVISION_NODE_COMMITTED.t()).asBoolean() : null;
        if (isCommittedObject == null) {
            throw new IllegalStateException("The revision node does not have defined flag about commit.");
        }
        isCommitted = isCommittedObject;

        commitTime = vertex.get(NodeProperty.REVISION_NODE_COMMIT_TIME.t()) != null ?
                vertex.get(NodeProperty.REVISION_NODE_COMMIT_TIME.t()).asString(): null;
    }

    /**
     * @return Číslo revize.
     */
    public Double getRevision() {
        return revision;
    }
    
    
    /**
     * @return Číslo revize včetně případné minor, tak jak se má zobrazovat
     */
    public String getDisplayNumber() {
        if (isMajor()) {
            return String.format("%.0f", Math.floor(revision));
        } else {
            double major = Math.floor(revision);
            double minor = Math.round((revision - major) * 1000000);
            return String.format("%.0f.%.0f", major, minor);
        }
    }
    
    
    /**
     * 
     * @return Previous revision number or null, if there is no previous revision
     */
    public Double getPreviousRevision() {
        return previousRevision;
    }

    /**
     * 
     * @return Next revision number or null, if there is no next revision
     */
    public Double getNextRevision() {
        return nextRevision;
    }

    /**
     * @return Čas commitnutí.
     */
    public String getCommitTime() {
        return commitTime;
    }

    /**
     * @return True, jestliže je revize commitnutá.
     */
    public boolean isCommitted() {
        return isCommitted;
    }
    
    /**
     * @return {@code true}, pokud je revize majoritni, jinak {@code false}.
     */
    public boolean isMajor() {
        return Math.floor(revision) == revision;
    }

    @Override
    public String toString() {
        return "Revision [#" + revision + "," + commitTime + "," + isCommitted + "]";
    }

    @Override
    public int compareTo(RevisionModel o) {
        return this.getRevision().compareTo(o.getRevision());
    }

    @Override
    public int hashCode() {
        return (revision == null) ? 0 : revision.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RevisionModel other = (RevisionModel) obj;
        if (revision == null) {
            if (other.revision != null) {
                return false;
            }
        } else if (!revision.equals(other.revision)) {
            return false;
        }
        return true;
    }
    
}