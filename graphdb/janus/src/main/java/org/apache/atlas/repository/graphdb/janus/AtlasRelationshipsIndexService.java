package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasRelationship;

import java.util.List;

public interface AtlasRelationshipsIndexService {

    void createRelationships(List<AtlasRelationship> relationships) throws AtlasBaseException;

    void deleteRelationship(AtlasRelationship relationship) throws AtlasBaseException;
}