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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.impl;

import java.util.ArrayList;
import java.util.List;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.IVgiPipelineConsumer;

public class ReadAllFeaturesConsumer implements IVgiPipelineConsumer {
	
	private List<IVgiFeature> featureList = new ArrayList<IVgiFeature>();

	public ReadAllFeaturesConsumer() {
	}
	
	@Override
	public void doBeforeFirstBatch() {
		featureList = new ArrayList<IVgiFeature>();
	}
	
	@Override
	public void handleBatch(List<IVgiFeature> batch) {
		featureList.addAll(batch);
	}

	@Override
	public void doAfterLastBatch() {}
	
	public List<IVgiFeature> getFeatureList() {
		return featureList;
	}
}
