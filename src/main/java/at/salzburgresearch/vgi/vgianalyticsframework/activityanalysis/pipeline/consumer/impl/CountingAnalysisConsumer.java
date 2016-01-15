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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.IVgiPipelineConsumer;

public class CountingAnalysisConsumer implements IVgiPipelineConsumer {
	private static Logger log = Logger.getLogger(CountingAnalysisConsumer.class);
	
	AtomicLong featureCount;
	AtomicLong operationCount;
	
	public CountingAnalysisConsumer() {
		featureCount = new AtomicLong(0);
		operationCount = new AtomicLong(0);
	}
	
	@Override
	public void doBeforeFirstBatch() { }
	
	@Override
	public void handleBatch(List<IVgiFeature> batch) {
		/** Count features in this batch */
		featureCount.getAndAdd(batch.size());
		
		/** Count operations in this batch */
		for (IVgiFeature feature : batch) {
			operationCount.getAndAdd(feature.getOperationList().size());
		}
		log.info(" > Operation Counter: Already processed " + featureCount.get() + " features with " + operationCount.get() + " operations");
	}
	
	@Override
	public void doAfterLastBatch() { }
}
