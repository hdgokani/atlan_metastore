package org.apache.atlas.ranger;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.ranger.plugin.model.RangerPolicy;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class RangerPolicyList {
	private static final long serialVersionUID = 1L;

	/**
	 * Start index for the result
	 */
	protected int startIndex;
	/**
	 * Page size used for the result
	 */
	protected int pageSize;
	/**
	 * Total records in the database for the given search conditions
	 */
	protected long totalCount;
	/**
	 * Number of rows returned for the search condition
	 */
	protected int resultSize;
	/**
	 * Sort type. Either desc or asc
	 */
	protected String sortType;
	/**
	 * Comma seperated list of the fields for sorting
	 */
	protected String sortBy;

	protected long queryTimeMS = System.currentTimeMillis();

	List<RangerPolicy> policies = new ArrayList<RangerPolicy>();

	public RangerPolicyList() {}

	public RangerPolicyList(List<RangerPolicy> objList) {
		int size = 0;
		if (objList != null) {
			size = objList.size();
		}

		startIndex = 0;
		pageSize = size;
		totalCount = size;
		resultSize = size;
		sortType = null;
		sortBy = null;

		this.policies = objList;
	}

	public List<RangerPolicy> getPolicies() {
		return policies;
	}

	public void setPolicies(List<RangerPolicy> policies) {
		this.policies = policies;
	}

	public int getListSize() {
		if (policies != null) {
			return policies.size();
		}
		return 0;
	}

	public List<?> getList() {
		return policies;
	}

	public int getStartIndex() {
		return startIndex;
	}

	public void setStartIndex(int startIndex) {
		this.startIndex = startIndex;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}

	public int getResultSize() {
		return resultSize;
	}

	public void setResultSize(int resultSize) {
		this.resultSize = resultSize;
	}

	public String getSortType() {
		return sortType;
	}

	public void setSortType(String sortType) {
		this.sortType = sortType;
	}

	public String getSortBy() {
		return sortBy;
	}

	public void setSortBy(String sortBy) {
		this.sortBy = sortBy;
	}

	public long getQueryTimeMS() {
		return queryTimeMS;
	}

	public void setQueryTimeMS(long queryTimeMS) {
		this.queryTimeMS = queryTimeMS;
	}
}
