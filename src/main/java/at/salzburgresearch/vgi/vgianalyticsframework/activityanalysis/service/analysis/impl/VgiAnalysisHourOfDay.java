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
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

/**
 * Analysis which determines which VGI contributor edits data at which day time
 *
 */
public class VgiAnalysisHourOfDay extends VgiAnalysisParent implements IVgiAnalysisAction {

	@Override
	public void analyze(IVgiAction action, Date timePeriod) {
		VgiAnalysisUser user = findUser(action.getOperations().get(0).getUid(), action.getOperations().get(0).getUser());
		IVgiOperation operation = action.getOperations().get(0);
		
		Calendar calA = Calendar.getInstance();
		calA.setTimeZone(TimeZone.getTimeZone("UTC"));
		calA.setTime(operation.getTimestamp());
		int hour = calA.get(Calendar.HOUR_OF_DAY);
		if (hour < 6) {
			hour = 0;
		} else if (hour < 12) {
			hour = 6;
		} else if (hour < 18) {
			hour = 12;
		} else {
			hour = 18;
		}
		Long l = user.actionTimestampPerHour.get(hour);
		if (l != null) {
			l = l + 1;
		} else {
			l = new Long(1l);
		}
		user.actionTimestampPerHour.put(hour, l);
	}

	@Override
	public void write(File path) {
		CSVFileWriter writer = new CSVFileWriter(path + "/action_timestamp_per_hour.csv");
		/** write header */
		writer.writeLine("uid;00_06;06_12;12_18;18_00;");
		/** iterate through rows*/
		for (Map.Entry<Integer, VgiAnalysisUser> user : userAnalysis.entrySet()) {
			/** write row values */
			VgiAnalysisUser u = user.getValue();
			
			String line = u.getUid() + ";";
			line += ((u.actionTimestampPerHour.containsKey(0)) ? u.actionTimestampPerHour.get(0) : "") + ";";
			line += ((u.actionTimestampPerHour.containsKey(6)) ? u.actionTimestampPerHour.get(6) : "") + ";";
			line += ((u.actionTimestampPerHour.containsKey(12)) ? u.actionTimestampPerHour.get(12) : "") + ";";
			line += ((u.actionTimestampPerHour.containsKey(18)) ? u.actionTimestampPerHour.get(18) : "") + ";";
			writer.writeLine(line);
		}
		writer.closeFile();
	}

	@Override
	public void reset() { }
	
	@Override
	public String toString() {
		return "VgiAnalysisHourOfDay";
	}

}
