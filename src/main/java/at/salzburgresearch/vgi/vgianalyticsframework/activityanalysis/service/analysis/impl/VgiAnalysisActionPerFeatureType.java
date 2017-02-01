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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.opengis.feature.simple.SimpleFeatureType;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

/**
 * Analysis which determines the number of actions per feature type
 *
 */
public class VgiAnalysisActionPerFeatureType extends VgiAnalysisParent implements IVgiAnalysisAction {
	
	private List<SimpleFeatureType> featureTypes = new ArrayList<SimpleFeatureType>();
	
	/** Constructor */
	public VgiAnalysisActionPerFeatureType(IVgiPipelineSettings settings) {
		this.settings = settings;
	}
	
	@Override
	public void analyze(IVgiAction action, Date timePeriod) {
		VgiAnalysisUser user = findUser(action.getOperations().get(0).getUid(), action.getOperations().get(0).getUser());
		
		if (!user.actionPerFeatureType.containsKey(timePeriod)) {
			user.actionPerFeatureType.put(timePeriod, new ConcurrentHashMap<SimpleFeatureType, Integer>());
		}
		Map<SimpleFeatureType, Integer> actionsPerFeatureType = user.actionPerFeatureType.get(timePeriod);
		
		if (!featureTypes.contains(action.getFeatureType())) featureTypes.add(action.getFeatureType());
		
		Integer value = actionsPerFeatureType.get(action.getFeatureType());
		if (value != null) {
			value = value + 1;
		} else {
			value = new Integer(1);
		}
		actionsPerFeatureType.put(action.getFeatureType(), value);
	}
	
	@Override
	public void write(File path) {
		try (CSVFileWriter writer = new CSVFileWriter(path + "/action_per_featuretype.csv")) {
			/** write header */
			String line = "";
			for (SimpleFeatureType featureType : featureTypes) {
				line += featureType.getName().getLocalPart() + ";";
			}
			writer.writeLine("uid;time_period;"+line);
			/** iterate through rows*/
			for (Map.Entry<Integer, VgiAnalysisUser> user : userAnalysis.entrySet()) {
				for (Entry<Date, Map<SimpleFeatureType, Integer>> featureType : user.getValue().actionPerFeatureType.entrySet()) {
	
					Map<SimpleFeatureType, Integer> m = featureType.getValue();
					
					line = "";
					for (SimpleFeatureType tag : featureTypes) {
						line += (m.containsKey(tag)) ? m.get(tag) : "";
						line += ";";
					}
					writer.writeLine(user.getValue().getUid() + ";" + dateFormat.format(featureType.getKey()) + ";" + line);
				}
			}
		} catch (IOException e) {
			log.error("Error while writing CSV file", e);
		}
	}

	@Override
	public void reset() {
		featureTypes.clear();
	}
	
	@Override
	public String toString() {
		return "VgiAnalysisFeatureType";
	}
}
