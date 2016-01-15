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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

/**
 * Analysis which determines the number of actions per contributor and action type
 *
 */
public class VgiAnalysisUserPerAction extends VgiAnalysisParent implements IVgiAnalysisAction {
	
	private boolean mergeActionTypes = false;
	
	private List<String> actionTypes = new ArrayList<String>();
	
	/** Constructor */
	public VgiAnalysisUserPerAction(IVgiPipelineSettings settings) {
		this.settings = settings;
	}
	
	@Override
	public void analyze(IVgiAction action, Date timePeriod) {
		VgiAnalysisUser user = findUser(action.getOperations().get(0).getUid(), action.getOperations().get(0).getUser());
		
		String actionType = (mergeActionTypes) ? "AC_UNDEFINED" : action.getActionName();
		
		Map<Date, Integer> m = user.actionCount.get(actionType);
		if (m == null) {
			user.actionCount.put(actionType, new ConcurrentHashMap<Date, Integer>());
			m = user.actionCount.get(actionType);
		}
		Integer n = m.get(timePeriod);
		if (n == null) {
			m.put(timePeriod, 0);
			n = m.get(timePeriod);
		}
		m.put(timePeriod, n + 1);
	}
	
	@Override
	public void write(File path) {
		CSVFileWriter writer = new CSVFileWriter(path + "/user_per_action.csv");
		
		Collections.sort(actionTypes);
		
		/** write header */
		String actionHeader = "uid;time_period";
		for (String a : actionTypes) {
			actionHeader += ";"+a;
		}
		writer.writeLine(actionHeader);
		
		/** iterate through users*/
		for (int user : userAnalysis.keySet()) {
			List<Date> writtenDates = new ArrayList<Date>();
			
			VgiAnalysisUser u = userAnalysis.get(user);
			for (String type : u.actionCount.keySet()) {
				for (Date period : u.actionCount.get(type).keySet()) {
					if (writtenDates.contains(period)) continue;
					
					if (u.actionCount.get(type).get(period) > 0) {
						String line = u.getUid() + ";" + dateFormat.format(period);
						long count = 0;
						for (String a : actionTypes) {
							Map<Date, Integer> actionCountInAllPeriods = u.actionCount.get(a);
							
							if (actionCountInAllPeriods != null) {
								Integer actionCountInPeriod = actionCountInAllPeriods.get(period);
								if (actionCountInPeriod != null) {
									line += ";"+decimalFormat.format(actionCountInPeriod);
									count += actionCountInPeriod;
								} else {
									line += ";";
								}
							} else {
								line += ";";
							}
							writtenDates.add(period);
						}
						line += "";
						if (count>0) writer.writeLine(line);
					}
				}
			}
		}
		writer.closeFile();
	}

	@Override
	public void reset() {
		actionTypes.clear();
		
		if (mergeActionTypes) {
			actionTypes.add("AC_UNDEFINED");
		} else {
			for (IVgiAction actionType : settings.getActionDefinitionList()) {
				actionTypes.add(actionType.getActionName());
			}
		}
	}
	
	@Override
	public String toString() {
		return "VgiAnalysisUserPerAction (mergeActionTypes=" + mergeActionTypes + ")";
	}

	public boolean isMergeActionTypes() {
		return mergeActionTypes;
	}

	public void setMergeActionTypes(boolean mergeActionTypes) {
		this.mergeActionTypes = mergeActionTypes;
	}
}
