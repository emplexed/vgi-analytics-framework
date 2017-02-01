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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

/**
 * Determines number of contributors, average number of actions and the
 * contributor with most actions
 *
 */
public class VgiAnalysisBatchContributor extends VgiAnalysisParent implements IVgiAnalysisAction {
	
	private List<AnalysisEntry> entryList = new ArrayList<AnalysisEntry>();
	
	private List<Date> timePeriods = new ArrayList<Date>();
	private List<String> actionTypes = new ArrayList<String>();
	private List<String> featureTypes = new ArrayList<String>();
    
    private AnalysisEntry currentEntry = null;;

	/** Constructor */
	public VgiAnalysisBatchContributor(IVgiPipelineSettings settings) {
		this.settings = settings;
	}
	
	@Override
	public void analyze(IVgiAction action, Date timePeriod) { }

	@Override
	public void write(File path) {
		if (settings.getCurrentPolygon() != null) {
			currentEntry.name = settings.getCurrentPolygon().getLabel();
		}
		
		currentEntry.numUser = VgiAnalysisParent.userAnalysis.size();
		long actionCount = 0;
		
		for (Entry<Integer, VgiAnalysisUser> user : VgiAnalysisParent.userAnalysis.entrySet()) {
			/** count actions */
			long userActionCount = 0;
			for (Entry<String, Map<Date, Integer>> action : user.getValue().actionCount.entrySet()) {
				for (Entry<Date, Integer> date : action.getValue().entrySet()) {
					userActionCount += date.getValue();
				}
			}
			
			/** Add user action count to overall action count */
			actionCount += userActionCount;
			
			/** Is this user the top user? */
			if (userActionCount > currentEntry.topUserActionCount) {
				currentEntry.topUserActionCount = userActionCount;
				currentEntry.topUserId = user.getKey();
			}
		}
		currentEntry.numActions = actionCount;
		
		entryList.add(currentEntry);
		
		try (CSVFileWriter writer = new CSVFileWriter(path + "/analysis_batch_summary.csv")) {
			/** write header */
			String heading = "region;num_user;num_actions;avg_actions_per_user;top_user_id;top_user_action_count;";
			
			Collections.sort(timePeriods);
	
			for (String featureType : featureTypes) {
				for (Date timePeriod : timePeriods) {
					for (String actionType : actionTypes) {
						heading += featureType + "_" + dateFormat.format(timePeriod) + "_" + actionType + ";";
					}
				}
			}
			writer.writeLine(heading);
			
			/** iterate through rows*/
			for (AnalysisEntry entry : entryList) {
				
				/** write row values */
				String line = entry.name + ";";
				line += entry.numUser + ";";
				line += entry.numActions + ";";
				line += ((entry.numUser > 0) ? Math.round(entry.numActions / entry.numUser) : 0) + ";";
				line += entry.topUserId + ";";
				line += entry.topUserActionCount + ";";
	
				writer.writeLine(line);
			}
		} catch (IOException e) {
			log.error("Error while writing CSV file", e);
		}
	}

	@Override
	public void reset() {
		currentEntry = new AnalysisEntry();
		
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
		private int numUser = 0;
		private long numActions = 0;
		private int topUserId = -1;
		private long topUserActionCount = 0;
	}

}
