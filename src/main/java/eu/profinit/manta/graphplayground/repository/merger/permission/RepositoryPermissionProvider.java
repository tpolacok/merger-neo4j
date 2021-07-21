package eu.profinit.manta.graphplayground.repository.merger.permission;

import org.neo4j.driver.types.Node;

/**
 * Repository access rights provider.
 *
 * @author onouza
 */
public interface RepositoryPermissionProvider {

    /**
     * Determines whether everyone has rights for whole repository.
     * @return {@code true} if everyone has rights, {@code false} otherwise
     */
    public boolean hasEveryoneAllRepositoryPermission();

    /**
     * Determines whether the current user has rights for the given vertex.
     * @param vertex Given vertex.
     * @return {@code true} if the user has rights, {@code false} otherwise
     */
    boolean hasCurrentUserVertexPermission(Node vertex);

    /**
     * Refreshes the permissions.
     */
    void refreshPermissions();
}
