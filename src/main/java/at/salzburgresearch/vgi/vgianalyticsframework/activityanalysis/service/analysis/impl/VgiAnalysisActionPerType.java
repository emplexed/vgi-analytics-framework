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
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

/**
 * Analysis which determines number of actions per type
 *
 */
public class VgiAnalysisActionPerType extends VgiAnalysisParent implements IVgiAnalysisAction {
	private Map<String, Map<Date, Long>> actionsPerType = new ConcurrentHashMap<String, Map<Date, Long>>();
	
	/** Constructor */
	public VgiAnalysisActionPerType(IVgiPipelineSettings settings) {
		this.settings = settings;
	}
	
	@Override
	public void analyze(IVgiAction action, Date timePeriod) {
		Map<Date, Long> m = actionsPerType.get(action.getActionName());
		if (!m.containsKey(timePeriod)) m.put(timePeriod, 0l);
		m.put(timePeriod, m.get(timePeriod)+1);
	}
	
	@Override
	public void write(File path) {
		CSVFileWriter writer = new CSVFileWriter(path + "/action_per_type.csv");
		/** write header */
		writer.writeLine("action_type;time_period;count;");
		/** iterate through rows*/
		for (String type : actionsPerType.keySet()) {
			for (Date period : actionsPerType.get(type).keySet()) {
				/** write row values */
				if (actionsPerType.get(type).get(period) > 0) {
					writer.writeLine(type + ";" + dateFormat.format(period) + ";" + actionsPerType.get(type).get(period) + ";");
				}
			}
		}
		writer.closeFile();
	}

	@Override
	public void reset() {
		actionsPerType.clear();
		for (IVgiAction type : settings.getActionDefinitionList()) {
			actionsPerType.put(type.getActionName(), new ConcurrentHashMap<Date, Long>());
		}
	}
	
	@Override
	public String toString() {
		return "VgiAnalysisActionPerType";
	}
}
