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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiFeatureImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.IVgiOperationPbfWriter;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.IVgiPipelineConsumer;

public class SortFeaturesConsumer implements IVgiPipelineConsumer {
	private static Logger log = Logger.getLogger(SortFeaturesConsumer.class);
	
	@Autowired
	private ApplicationContext ctx;
	
	@Autowired
	@Qualifier("vgiPipelineSettings")
	private IVgiPipelineSettings settings;

	private File path = null;
	
	private List<IVgiFeature> featureList = new ArrayList<IVgiFeature>();
	
	public SortFeaturesConsumer() {
	}
	
	@Override
	public void doBeforeFirstBatch() { }
	
	@Override
	public void handleBatch(List<IVgiFeature> batch) {
		featureList.addAll(batch);
	}
	
	@Override
	public void doAfterLastBatch() {
		log.info("Sort features - Start " + path);
		Collections.sort(featureList, VgiFeatureImpl.getFeatureComparator());
		for (IVgiFeature feature : featureList) {
			Collections.sort(feature.getOperationList(), VgiOperationImpl.getVersionComparator());
		}
		log.info("Sort features - End");
		
		log.info("Write features - Start " + path);
		
		if (!path.exists()) path.mkdir();
		if (featureList.size() == 0) return;
		
		/** Load PBF writer */
		IVgiOperationPbfWriter writer = ctx.getBean("vgiOperationPbfWriter", IVgiOperationPbfWriter.class);
		writer.initializePbfWriterToAppend(path);
		/** Write features */
		writer.writePbfFeatures(featureList);
		/** Terminates PBF Writer (write index file, ... */
		writer.terminatePbfWriter();
		
		/** Delete features in quadtree (now they are only in PBF file) */
		featureList.clear();
		
		log.info("Write features - Finish");
	}

	public void setPath(File path) {
		this.path = path;
	}
}
