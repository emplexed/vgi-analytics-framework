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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiActionImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiActionImpl.ActionType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

public class VgiAnalysisUpdate extends VgiAnalysisParent implements IVgiAnalysisAction {
	
	private String tagKey = "";
	
	private Map<Date, UpdateEntry> data = new HashMap<Date, UpdateEntry>();
	
	/** Constructor */
	public VgiAnalysisUpdate(IVgiPipelineSettings settings) {
		this.settings = settings;
	}
	
	@Override
	public void analyze(IVgiAction action, Date timePeriod) {
		if (!data.containsKey(timePeriod)) {
			data.put(new Date(timePeriod.getTime()), new UpdateEntry());
		}
		
		if (action.getActionType().equals(ActionType.CREATE)) {
			data.get(timePeriod).feature_count++;
		} else if (action.getActionType().equals(ActionType.DELETE)) {
			data.get(timePeriod).feature_count--;
		} else if (action.getActionType().equals(ActionType.UPDATE)) {
			if (!data.get(timePeriod).action_count_updated.containsKey(action.getActionName())) {
				data.get(timePeriod).action_count_updated.put(action.getActionName(), new Integer(0));
			}
			int i = data.get(timePeriod).action_count_updated.get(action.getActionName());
			data.get(timePeriod).action_count_updated.put(action.getActionName(), ++i);
			if (!data.get(timePeriod).featureId_last_affected.equals(action.getOperations().get(0).getOid())) {
				data.get(timePeriod).feature_count_affected++;
				data.get(timePeriod).featureId_last_affected = action.getOperations().get(0).getOid();
			}
		}
	}
	
	@Override
	public void write(File path) {
		try (CSVFileWriter writer = new CSVFileWriter(path + "/update.csv")) {
			/** write header */
			String header = "year;feature_count_delta;feature_count_updated";
			for (IVgiAction action : settings.getActionDefinitionList()) {
				if (!action.getActionType().equals(VgiActionImpl.ActionType.UPDATE)) continue;
				header += ";" + action.getActionName();
			}
			writer.writeLine(header);
			/** iterate through rows*/
			for (Entry<Date, UpdateEntry> dataEntry : data.entrySet()) {
				String line = dateFormatYear.format(dataEntry.getKey()) + ";" + dataEntry.getValue().feature_count + ";" + dataEntry.getValue().feature_count_affected;
				for (IVgiAction action : settings.getActionDefinitionList()) {
					if (!action.getActionType().equals(VgiActionImpl.ActionType.UPDATE)) continue;
					line += ";" + dataEntry.getValue().action_count_updated.get(action.getActionName());
				}
				writer.writeLine(line);
			}
		} catch (IOException e) {
			log.error("Error while writing CSV file", e);
		}
	}
	
	@Override
	public void reset() { 
		data = new HashMap<Date, UpdateEntry>();
	}
	
	@Override
	public String toString() {
		return "VgiAnalysisTags (tagKey=" + tagKey + ")";
	}

	public String getTagKey() {
		return tagKey;
	}

	public void setTagKey(String tagKey) {
		this.tagKey = tagKey;
	}
	
	private class UpdateEntry {
		int feature_count = 0;
		Long featureId_last_affected = -1l;
		int feature_count_affected = 0;
		Map<String, Integer> action_count_updated = new HashMap<String, Integer>(); //VgiActionType, count
		
	}
}
