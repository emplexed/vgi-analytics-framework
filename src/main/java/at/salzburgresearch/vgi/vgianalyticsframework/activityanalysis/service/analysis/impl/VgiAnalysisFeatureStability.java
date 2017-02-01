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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

public class VgiAnalysisFeatureStability extends VgiAnalysisParent implements IVgiAnalysisFeature {
	
	private Map<Long, Integer> featureStability = new ConcurrentHashMap<Long, Integer>();
	
	/** Constructor */
	public VgiAnalysisFeatureStability(IVgiPipelineSettings settings) {
		this.settings = settings;
	}
	
	@Override
	public void analyze(IVgiFeature feature, Date timePeriod) {
		long durationUntilEdit = Long.MAX_VALUE;
		if (feature.getActionList().size() > 1) {
			durationUntilEdit = feature.getActionList().get(1).getOperations().get(0).getTimestamp().getTime() - 
					feature.getActionList().get(0).getOperations().get(0).getTimestamp().getTime();
		}
		
		durationUntilEdit = (long) durationUntilEdit / (1000*60*60);
		
		if (!featureStability.containsKey(durationUntilEdit)) featureStability.put(durationUntilEdit, 0);
		featureStability.put(durationUntilEdit, featureStability.get(durationUntilEdit)+1);
	}
	
	@Override
	public void write(File path) {
		try (CSVFileWriter writer = new CSVFileWriter(path + "/feature_stability.csv")) {
			/** write header */
			writer.writeLine("durationUntilEdit;featureCount");
			/** iterate through rows*/
			for (Long durationUntilEdit : featureStability.keySet()) {
				/** write row values */
				writer.writeLine(durationUntilEdit + ";" + featureStability.get(durationUntilEdit));
			}
		} catch (IOException e) {
			log.error("Error while writing CSV file", e);
		}
	}

	@Override
	public void reset() {
		featureStability.clear();
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName();
	}
}
