package org.apache.atlas.discovery;

import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.lineage.LineageOnDemandConstraints;
import org.apache.atlas.model.lineage.LineageOnDemandDefaultParams;
import org.apache.atlas.model.lineage.LineageOnDemandRequest;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.Predicate;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AtlasLineageOnDemandContext {
    private Map<String, LineageOnDemandConstraints> constraints;
    private Predicate                               predicate;
    private Set<String>                             attributes;
    private Set<String>                             relationAttributes;
    private boolean                                 hideProcess;
    private LineageOnDemandDefaultParams            defaultParams;

    public AtlasLineageOnDemandContext(LineageOnDemandRequest lineageOnDemandRequest, AtlasTypeRegistry typeRegistry) {
        this.constraints = lineageOnDemandRequest.getConstraints();
        this.attributes = lineageOnDemandRequest.getAttributes();
        this.relationAttributes = lineageOnDemandRequest.getRelationAttributes();
        this.hideProcess = lineageOnDemandRequest.isHideProcess();
        this.defaultParams = lineageOnDemandRequest.getDefaultParams();
        this.predicate = constructInMemoryPredicate(typeRegistry, lineageOnDemandRequest.getTraversalFilters());
    }

    public Map<String, LineageOnDemandConstraints> getConstraints() {
        return constraints;
    }

    public void setConstraints(Map<String, LineageOnDemandConstraints> constraints) {
        this.constraints = constraints;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public void setPredicate(Predicate predicate) {
        this.predicate = predicate;
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<String> attributes) {
        this.attributes = attributes;
    }

    public Set<String> getRelationAttributes() {
        return relationAttributes;
    }

    public void setRelationAttributes(Set<String> relationAttributes) {
        this.relationAttributes = relationAttributes;
    }

    public boolean isHideProcess() {
        return hideProcess;
    }

    public void setHideProcess(boolean hideProcess) {
        this.hideProcess = hideProcess;
    }

    public LineageOnDemandDefaultParams getDefaultParams() {
        return defaultParams;
    }

    public void setDefaultParams(LineageOnDemandDefaultParams defaultParams) {
        this.defaultParams = defaultParams;
    }

    protected Predicate constructInMemoryPredicate(AtlasTypeRegistry typeRegistry, List<SearchParameters.FilterCriteria> filterCriteriaList) {
        LineageSearchProcessor lineageSearchProcessor = new LineageSearchProcessor();
        return lineageSearchProcessor.constructInMemoryPredicate(typeRegistry, filterCriteriaList);
    }

    protected boolean evaluate(AtlasVertex vertex) {
        if (predicate != null) {
            return predicate.evaluate(vertex);
        }
        return true;
    }
}
