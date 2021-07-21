package eu.profinit.manta.graphplayground.repository.merger.permission;

import org.neo4j.driver.types.Node;

/**
 * Singleton repository access rights provider.
 *
 * @author onouza
 */
public final class AllRepositoryPermissionProvider implements RepositoryPermissionProvider {
    
    /**
     * Singleton instance.
     */
    private static final AllRepositoryPermissionProvider INSTANCE = new AllRepositoryPermissionProvider();

    private AllRepositoryPermissionProvider() {}
    
    /**
     * @return Singleton instance.
     */
    public static AllRepositoryPermissionProvider getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean hasEveryoneAllRepositoryPermission() {
        return true;
    }

    @Override
    public boolean hasCurrentUserVertexPermission(Node vertex) {
        return true;
    }

    @Override
    public void refreshPermissions() {}

}
