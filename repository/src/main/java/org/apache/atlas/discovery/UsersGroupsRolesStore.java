package org.apache.atlas.discovery;

import org.apache.atlas.plugin.util.RangerRoles;
import org.apache.atlas.plugin.util.RangerUserStore;

public class UsersGroupsRolesStore {

    private RangerUserStore userStore;
    private RangerRoles allRoles;
    private static UsersGroupsRolesStore usersGroupsRolesStore;

    public static UsersGroupsRolesStore getInstance() {
        synchronized (UsersGroupsRolesStore.class) {
            if (usersGroupsRolesStore == null) {
                usersGroupsRolesStore = new UsersGroupsRolesStore();
            }
            return usersGroupsRolesStore;
        }
    }

    public UsersGroupsRolesStore () {}

    public void setUserStore(RangerUserStore userStore) {
        this.userStore = userStore;
    }

    public RangerUserStore getUserStore() {
        return userStore;
    }

    public void setAllRoles(RangerRoles allRoles) {
        this.allRoles = allRoles;
    }

    public RangerRoles getAllRoles() {
        return allRoles;
    }
}
