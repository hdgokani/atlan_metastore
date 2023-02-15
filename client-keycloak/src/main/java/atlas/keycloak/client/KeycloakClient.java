/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package atlas.keycloak.client;

import org.apache.commons.collections.CollectionUtils;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.GroupRepresentation;
import org.keycloak.representations.idm.RealmRepresentation;
import org.keycloak.representations.idm.RoleRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.omg.CORBA.PRIVATE_MEMBER;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//@Component
public class KeycloakClient {
    public static final Logger LOG = LoggerFactory.getLogger(KeycloakClient.class);

    private static KeycloakClient keycloakClient = null;
    private static Keycloak keycloak = null;

    //TODO: get creds from properties
    private static String REALM_ID = "default";
    private static String AUTH_SERVER_URL = "https://beta.atlan.dev/auth";
    private static String CLIENT_ID = "atlan-backend";
    private static String CLIENT_SECRET = "f176d49a-d82f-4d41-a05e-11940c076fcd";
    private static String GRANT_TYPE = "client_credentials";


    private KeycloakClient() {
    }

    public static KeycloakClient getKeycloakClient() {
        if (keycloakClient == null) {
            LOG.info("Initializing Keycloak client..");
            init();
            LOG.info("Initialized Keycloak client..");
        }
        return keycloakClient;
    }

    private static void init() {
        synchronized (KeycloakClient.class) {
            if (keycloakClient == null) {
                keycloak = KeycloakBuilder.builder()
                        .serverUrl(AUTH_SERVER_URL)
                        .realm(REALM_ID)
                        .clientId(CLIENT_ID)
                        .clientSecret(CLIENT_SECRET)
                        .grantType(GRANT_TYPE)
                        .resteasyClient(new ResteasyClientBuilder().build())
                        .build();

                keycloakClient = new KeycloakClient();
            }
        }
    }

    public RealmResource getRealm() {
        return keycloak.realm(REALM_ID);
    }

    public List<UserRepresentation> getAllUsers() {
        int start = 0;
        int size = 100;

        List<UserRepresentation> ret = new ArrayList<>();

        do {
            List<UserRepresentation> userRepresentations = getRealm().users().list(start, size);
            ret.addAll(userRepresentations);
            start += size;

        } while (CollectionUtils.isNotEmpty(ret) && ret.size() % size == 0);

        return ret;
    }

    public List<GroupRepresentation> getAllGroups() {

        int start = 0;
        int size = 100;

        List<GroupRepresentation> ret = new ArrayList<>();

        do {
            List<GroupRepresentation> groupRepresentations = getRealm().groups().groups(start, size);
            ret.addAll(groupRepresentations);
            start += size;

        } while (CollectionUtils.isNotEmpty(ret) && ret.size() % size == 0);

        return ret;
    }

    public List<RoleRepresentation> getAllRoles() {
        int start = 0;
        int size = 100;

        List<RoleRepresentation> ret = new ArrayList<>();

        do {
            List<RoleRepresentation> roleRepresentations = getRealm().roles().list(start, size);
            ret.addAll(roleRepresentations);
            start += size;

        } while (CollectionUtils.isNotEmpty(ret) && ret.size() % size == 0);

        return ret;
    }
}
