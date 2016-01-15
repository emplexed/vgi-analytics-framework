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

import gnu.trove.TIntArrayList;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opengis.feature.simple.SimpleFeatureType;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiActionImpl.ActionType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

/**
 * General batch analysis!<br>
 * Determines action count, number of affected features and number of
 * contributors per region, feature type, time period and action type
 *
 */
public class VgiAnalysisBatchGeneral extends VgiAnalysisParent implements IVgiAnalysisAction {
	
	private List<AnalysisEntry> entryList = new ArrayList<AnalysisEntry>();
	
	private List<Date> timePeriods = new ArrayList<Date>();
	private List<String> actionTypes = new ArrayList<String>();
	private List<SimpleFeatureType> featureTypes = new ArrayList<SimpleFeatureType>();
    
    private AnalysisEntry currentEntry = null;
    
	/** Constructor */
	public VgiAnalysisBatchGeneral(IVgiPipelineSettings settings) {
		this.settings = settings;

		/** write header */
		CSVFileWriter writer = new CSVFileWriter(settings.getResultFolder() + "/analysis_batch_feature_type.csv", true);
		writer.writeLine("region;feature_type;time_period;action_type;action_count;affected_count;user_count;");
		writer.closeFile();
	}
	
	@Override
	public void analyze(IVgiAction action, Date timePeriod) {
		/** Select time period */
		if (!currentEntry.actionCount.containsKey(timePeriod)) {
			currentEntry.actionCount.put(timePeriod, new HashMap<SimpleFeatureType, Map<String, AnalysisEntryUser>>());
			
			if (!timePeriods.contains(timePeriod)) timePeriods.add(timePeriod);
		}
		Map<SimpleFeatureType, Map<String, AnalysisEntryUser>> entryTime = currentEntry.actionCount.get(timePeriod);
		
		/** Select feature type */
		if (!entryTime.containsKey(action.getFeatureType())) {
			entryTime.put(action.getFeatureType(), new HashMap<String, AnalysisEntryUser>());
			
			if (!featureTypes.contains(action.getFeatureType())) featureTypes.add(action.getFeatureType());
		}
		Map<String, AnalysisEntryUser> entryTimeFeatureType = entryTime.get(action.getFeatureType());
		
		/** Select action type */
		if (!entryTimeFeatureType.containsKey(action.getActionName())) {
			entryTimeFeatureType.put(action.getActionName(), new AnalysisEntryUser());
		}
		AnalysisEntryUser entryTimeActionType = entryTimeFeatureType.get(action.getActionName());
		
		entryTimeActionType.value = entryTimeActionType.value + 1;
		if (!entryTimeActionType.userList.contains(action.getOperations().get(0).getUid())) {
			entryTimeActionType.userList.add(action.getOperations().get(0).getUid());
		}
		
		if (action.getActionType().equals(ActionType.CREATE)) {
			entryTimeActionType.feature_count_cum++;
		} else if (action.getActionType().equals(ActionType.DELETE)) {
			entryTimeActionType.feature_count_cum--;
		}
		
		if (!entryTimeActionType.featureId_last_affected.equals(action.getOperations().get(0).getOid())) {
			entryTimeActionType.feature_count_affected++;
			entryTimeActionType.featureId_last_affected = action.getOperations().get(0).getOid();
		}
	}
	
	@Override
	public void write(File path) {
		currentEntry.name = settings.getFilterPolygonLabel();
		entryList.add(currentEntry);
		
		CSVFileWriter writer = new CSVFileWriter(settings.getResultFolder() + "/analysis_batch_feature_type.csv", true);
		
		Collections.sort(timePeriods);

		/** iterate through rows*/
		for (AnalysisEntry entry : entryList) {
			
			/** write row values */
			for (SimpleFeatureType featureType : featureTypes) {
				for (Date timePeriod : timePeriods) {
					for (String actionType : actionTypes) {
						
						if (!entry.actionCount.containsKey(timePeriod)) continue;
						if (!entry.actionCount.get(timePeriod).containsKey(featureType)) continue;
						if (!entry.actionCount.get(timePeriod).get(featureType).containsKey(actionType)) continue;

						/** Write row if value is > 0 */
						int value = entry.actionCount.get(timePeriod).get(featureType).get(actionType).value;
						if (value > 0) {
							String line = "";
							line += entry.name + ";" + featureType.getName().getLocalPart() + ";" + dateFormat.format(timePeriod) + ";" + actionType + ";";
							line += value + ";";
							line += entry.actionCount.get(timePeriod).get(featureType).get(actionType).feature_count_affected + ";";
							line += entry.actionCount.get(timePeriod).get(featureType).get(actionType).userList.size();
							writer.writeLine(line);
						}
					}
				}
			}
		}
		writer.closeFile();
	}
	
	@Override
	public void reset() {
		currentEntry = new AnalysisEntry();
		
		actionTypes.clear();
		entryList = new ArrayList<AnalysisEntry>();
		
		for (IVgiAction actionType : settings.getActionDefinitionList()) {
			if (!actionTypes.contains(actionType.getActionName())) actionTypes.add(actionType.getActionName());
		}
	}
	
	@Override
	public String toString() {
		return "VgiAnalysisBatchFeatureType";
	}
	
	private class AnalysisEntry {
		private String name = "";
		private Map<Date, Map<SimpleFeatureType, Map<String, AnalysisEntryUser>>> actionCount = new HashMap<Date, Map<SimpleFeatureType, Map<String, AnalysisEntryUser>>>();
	}
	
	private class AnalysisEntryUser {
		private int value;
		private Long feature_count_cum = 0l;
		private Long feature_count_affected = 0l;
		private Long featureId_last_affected = -1l;
		private TIntArrayList userList = new TIntArrayList();
	}
}
