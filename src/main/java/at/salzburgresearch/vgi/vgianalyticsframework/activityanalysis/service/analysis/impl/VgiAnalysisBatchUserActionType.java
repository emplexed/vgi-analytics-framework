/** Copyright 2017, Simon Gröchenig, Salzburg Research Forschungsgesellschaft m.b.H.

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

public class VgiAnalysisBatchUserActionType extends VgiAnalysisParent implements IVgiAnalysisAction {
	
	private List<AnalysisEntry> entryList = new ArrayList<AnalysisEntry>();
	
	private List<String> actionTypes = new ArrayList<String>();

	/** Constructor */
	public VgiAnalysisBatchUserActionType(IVgiPipelineSettings settings) {
		this.settings = settings;
	}
	
	@Override
	public void analyze(IVgiAction action, Date timePeriod) { }
	
	@Override
	public void write(File path) {
		AnalysisEntry currentEntry = new AnalysisEntry();
		if (settings.getCurrentPolygon() != null) {
			currentEntry.name = settings.getCurrentPolygon().getLabel();
		}
		
//		for (Entry<Integer, VgiAnalysisUser> user : VgiAnalysisParent.userAnalysis.entrySet()) { //TODO repair
//
//			for (Entry<String, Map<Date, Integer>> action : user.getValue().actionCount.entrySet()) {
//				for (Entry<Date, Integer> timePeriod : action.getValue().entrySet()) {
//					
//					if (!currentEntry.userCount.containsKey(timePeriod.getKey())) currentEntry.userCount.put(timePeriod.getKey(), new ArrayList<Integer>());
//					if (!currentEntry.actionCount.containsKey(timePeriod.getKey())) currentEntry.actionCount.put(timePeriod.getKey(), new HashMap<String, Integer>());
//					if (!currentEntry.actionCount.get(timePeriod.getKey()).containsKey(action.getKey())) currentEntry.actionCount.get(timePeriod.getKey()).put(action.getKey(), 0);
//					
//					Map<String, Integer> entryTime = currentEntry.actionCount.get(timePeriod.getKey());
//					entryTime.put(action.getKey(), entryTime.get(action.getKey()).longValue() + timePeriod.getValue());
//
//					List<Integer> entryUser = currentEntry.userCount.get(timePeriod.getKey());
//					if (!entryUser.contains(user.getKey())) entryUser.add(user.getKey());
//				}
//			}
//		}
		
		entryList.add(currentEntry);
		
		try (CSVFileWriter writer = new CSVFileWriter(path + "/analysis_batch_user_action_type.csv")) {
			/** write header */
			String heading = "region;time_period;num_user;";
		
			for (String actionType : actionTypes) {
				heading += actionType + ";";
			}
			writer.writeLine(heading);
			
			/** iterate through rows*/
			for (AnalysisEntry entry : entryList) {
				
				for (Date timePeriod : entry.actionCount.keySet()) {
					String line = entry.name + ";";
					line += dateFormat.format(timePeriod) + ";";
					line += entry.userCount.get(timePeriod).size() + ";";
					
					for (String actionType : actionTypes) {
						if (entry.actionCount.get(timePeriod).containsKey(actionType)) {
							line += entry.actionCount.get(timePeriod).get(actionType) + ";";
						} else {
							line += ";";
						}
					}
					writer.writeLine(line);
				}
			}
		} catch (IOException e) {
			log.error("Error while writing CSV file", e);
		}
	}

	@Override
	public void reset() {
		actionTypes.clear();
		
		for (IVgiAction actionType : settings.getActionDefinitionList()) {
			actionTypes.add(actionType.getActionName());
		}
	}
	
	@Override
	public String toString() {
		return "VgiAnalysisSummary";
	}
	
	private class AnalysisEntry {
		private String name = "";
		private Map<Date, Map<String, Integer>> actionCount = new HashMap<Date, Map<String, Integer>>();
		private Map<Date, List<Integer>> userCount = new HashMap<Date, List<Integer>>();
	}

}
