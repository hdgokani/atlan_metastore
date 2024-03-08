/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.plugin.resourcematcher;


import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class RangerEntityResourceMatcher extends RangerAbstractResourceMatcher {
	private static final Log LOG = LogFactory.getLog(RangerEntityResourceMatcher.class);

	@Override
	public void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAbstractResourceMatcher.init()");
		}

		Map<String, String> options = resourceDef != null ? resourceDef.getMatcherOptions() : null;

		optIgnoreCase          = getOptionIgnoreCase(options);
		optQuotedCaseSensitive = getOptionQuotedCaseSensitive(options);
		optQuoteChars          = getOptionQuoteChars(options);
		optWildCard            = getOptionWildCard(options);

		policyValues = new ArrayList<>();
		policyIsExcludes = policyResource != null && policyResource.getIsExcludes();

		if (policyResource != null && policyResource.getValues() != null) {
			for (String policyValue : policyResource.getValues()) {
				if (StringUtils.isEmpty(policyValue)) {
					continue;
				}
				policyValues.add(policyValue);
			}
		}

		optReplaceTokens = getOptionReplaceTokens(options);

		if(optReplaceTokens) {
			startDelimiterChar = getOptionDelimiterStart(options);
			endDelimiterChar   = getOptionDelimiterEnd(options);
			escapeChar         = getOptionDelimiterEscape(options);
			tokenPrefix        = getOptionDelimiterPrefix(options);

			if(escapeChar == startDelimiterChar || escapeChar == endDelimiterChar ||
					tokenPrefix.indexOf(escapeChar) != -1 || tokenPrefix.indexOf(startDelimiterChar) != -1 ||
					tokenPrefix.indexOf(endDelimiterChar) != -1) {
				String resouceName = resourceDef == null ? "" : resourceDef.getName();

				String msg = "Invalid token-replacement parameters for resource '" + resouceName + "': { ";
				msg += (OPTION_TOKEN_DELIMITER_START + "='" + startDelimiterChar + "'; ");
				msg += (OPTION_TOKEN_DELIMITER_END + "='" + endDelimiterChar + "'; ");
				msg += (OPTION_TOKEN_DELIMITER_ESCAPE + "='" + escapeChar + "'; ");
				msg += (OPTION_TOKEN_DELIMITER_PREFIX + "='" + tokenPrefix + "' }. ");
				msg += "Token replacement disabled";

				LOG.error(msg);

				optReplaceTokens = false;
			}
		}

		resourceMatchers = buildResourceMatchers();
		isMatchAny = resourceMatchers == null || CollectionUtils.isEmpty(resourceMatchers.getResourceMatchers());

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAbstractResourceMatcher.init()");
		}
	}

	protected ResourceMatcherWrapper buildResourceMatchers() {
		List<ResourceMatcher> resourceMatchers = new ArrayList<>();
		boolean needsDynamicEval = false;

		for (String policyValue : policyValues) {
			ResourceMatcher matcher = getMatcher(policyValue);

			if (matcher != null) {
				if (matcher.isMatchAny()) {
					resourceMatchers.clear();
					break;
				}
				if (!needsDynamicEval && matcher.getNeedsDynamicEval()) {
					needsDynamicEval = true;
				}
				resourceMatchers.add(matcher);
			}
		}

		Collections.sort(resourceMatchers, new ResourceMatcher.PriorityComparator());

		return CollectionUtils.isNotEmpty(resourceMatchers) ?
				new ResourceMatcherWrapper(needsDynamicEval, resourceMatchers) : null;
	}

	@Override
	public boolean isMatch(Object resource, Map<String, Object> evalContext) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultResourceMatcher.isMatch(" + resource + ", " + evalContext + ")");
		}

		boolean ret = false;
		boolean allValuesRequested = isAllValuesRequested(resource);

		if(allValuesRequested || isMatchAny) {
			ret = isMatchAny;
		} else {
			int resourceMatcherCount = resourceMatchers.getResourceMatchers().size();
			int matchedResourceMatcherCount = 0;

			for (ResourceMatcher resourceMatcher : resourceMatchers.getResourceMatchers()) {
				Object value = ((Map<?, ?>) resource).get(resourceMatcher.attrName);

				if (value instanceof String) {
					boolean result = resourceMatcher.isMatch((String) value, evalContext);
					if (result) {
						matchedResourceMatcherCount++;
					}
				} else if (value instanceof Collection) {
					Collection<String> collValue = (Collection<String>) value;

					/*ret = resourceMatcher.isMatchAny(collValue, evalContext);
					if (ret) {
						break;
					}*/

					boolean result = resourceMatcher.isMatchAny(collValue, evalContext);
					if (result) {
						matchedResourceMatcherCount++;
					}
				}
			}

			if (matchedResourceMatcherCount == resourceMatcherCount) {
				ret = true; //all resources for entityAttr matched
			}
		}

		ret = applyExcludes(allValuesRequested, ret);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultResourceMatcher.isMatch(" + resource + ", " + evalContext + "): " + ret);
		}

		return ret;
	}

	ResourceMatcher getMatcher(String policyValueRaw) {
		final ResourceMatcher ret;

		if (policyValueRaw.equals("*")){
			return super.getMatcher(policyValueRaw);
		}

		String[] splitted = policyValueRaw.split(":");
		String attrName = splitted[0];
		String policyValue = splitted[1];

		switch (attrName) {
			case "certificateStatus":
				ret = super.getMatcher(policyValue); break;



			default:
				ret = super.getMatcher(policyValue);
		}
		ret.attrName = attrName;

		return ret;
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerDefaultResourceMatcher={");

		super.toString(sb);

		sb.append("}");

		return sb;
	}
}
