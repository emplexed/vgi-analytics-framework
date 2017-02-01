/** Copyright 2016, Simon Gröchenig, Salzburg Research Forschungsgesellschaft m.b.H.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opengis.feature.simple.SimpleFeatureType;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeatureType;

public class VgiFeatureTypeImpl implements IVgiFeatureType {
	private SimpleFeatureType featureType = null;
	private Map<String, List<String>> featureTypeTagsInclude = new HashMap<String, List<String>>();
	private Map<String, List<String>> featureTypeTagsExclude = new HashMap<String, List<String>>();
	
	@Override
	public SimpleFeatureType getFeatureType() {
		return featureType;
	}
	@Override
	public void setFeatureType(SimpleFeatureType featureType) {
		this.featureType = featureType;
	}
	
	@Override
	public Map<String, List<String>> getFeatureTypeTagsInclude() {
		return featureTypeTagsInclude;
	}
	@Override
	public void setFeatureTypeTagsInclude(Map<String, List<String>> tags) {
		this.featureTypeTagsInclude = tags;
	}
	
	@Override
	public Map<String, List<String>> getFeatureTypeTagsExclude() {
		return featureTypeTagsExclude;
	}
	@Override
	public void setFeatureTypeTagsExclude(Map<String, List<String>> tags) {
		this.featureTypeTagsExclude = tags;
	}
}
