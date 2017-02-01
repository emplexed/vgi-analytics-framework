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

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

/**
 * Analysis which determines number of operations per type
 *
 */
public class VgiAnalysisOperationPerType extends VgiAnalysisParent implements IVgiAnalysisOperation {	
	private Map<VgiOperationType, Map<Date, Long>> operationsPerType = new ConcurrentHashMap<VgiOperationType, Map<Date, Long>>();

	/** Constructor */
	public VgiAnalysisOperationPerType() {
	}
	
	@Override
	public void analyze(IVgiOperation operation, Date timePeriod) {
		Map<Date, Long> m = operationsPerType.get(operation.getVgiOperationType());
		if (!m.containsKey(timePeriod)) m.put(timePeriod, 0l);
		m.put(timePeriod, m.get(timePeriod)+1);
	}

	@Override
	public void write(File path) {
		try (CSVFileWriter writer = new CSVFileWriter(path + "/operation_per_type.csv")) {
			/** write header */
			writer.writeLine("operation_type;time_period;count;");
			/** iterate through rows*/
			for (VgiOperationType type : operationsPerType.keySet()) {		
				for (Date period : operationsPerType.get(type).keySet()) {
					/** write row values */
					if (operationsPerType.get(type).get(period) > 0) {
						writer.writeLine(type + ";" + dateFormat.format(period) + ";" + operationsPerType.get(type).get(period) + ";");
					}
				}
			}
		} catch (IOException e) {
			log.error("Error while writing CSV file", e);
		}
	}

	@Override
	public void reset() {
		operationsPerType.clear();
		for (VgiOperationType type : VgiOperationType.values()) {
			operationsPerType.put(type, new ConcurrentHashMap<Date, Long>());
		}
	}
	
	@Override
	public String toString() {
		return "VgiAnalysisOperationPerType";
	}
}
