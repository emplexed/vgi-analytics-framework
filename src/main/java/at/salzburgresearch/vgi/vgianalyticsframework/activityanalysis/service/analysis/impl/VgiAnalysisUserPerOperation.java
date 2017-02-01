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
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

/**
 * Analysis which determines the number of actions per contributor and action type
 *
 */
public class VgiAnalysisUserPerOperation extends VgiAnalysisParent implements IVgiAnalysisOperation {
	
	private boolean mergeOperationTypes = false;

	/** Constructor */
	public VgiAnalysisUserPerOperation(IVgiPipelineSettings settings) {
		this.settings = settings;
	}
	
	@Override
	public void analyze(IVgiOperation operation, Date timePeriod) {
		
		VgiAnalysisUser user = findUser(operation.getUid(), operation.getUser());
		
		VgiOperationType operationType = operation.getVgiOperationType();
		
		if (mergeOperationTypes) operationType = VgiOperationType.OP_UNDEFINED;

		Map<Date, Long> m = user.operationCount.get(operationType);
		if (m == null) {
			user.operationCount.put(operationType, new ConcurrentHashMap<Date, Long>());
			m = user.operationCount.get(operationType);
		}
		Long n = m.get(timePeriod);
		if (n == null) {
			m.put(timePeriod, 0l);
			n = m.get(timePeriod);
		}
		m.put(timePeriod, ++n);
	}
	
	@Override
	public void write(File path) {
		
		try (CSVFileWriter writer = new CSVFileWriter(path + "/user_per_operation.csv")) {
				
			String operationHeader = "uid;operation_type;time_period;count;";
			writer.writeLine(operationHeader);
			
			for (int user : userAnalysis.keySet()) {
				VgiAnalysisUser u1 = userAnalysis.get(user);
				for (VgiOperationType type : u1.operationCount.keySet()) {
					for (Date period : u1.operationCount.get(type).keySet()) {
						if (u1.operationCount.get(type).get(period) > 0) {
							writer.writeLine(u1.getUid() + ";" + type + ";" + dateFormat.format(period) + ";" + u1.operationCount.get(type).get(period) + ";");
						}
					}
				}
			}
		} catch (IOException e) {
			log.error("Error while writing CSV file", e);
		}
	}

	@Override
	public void reset() { }
	
	@Override
	public String toString() {
		return "VgiAnalysisUserPerOperation (mergeOperationTypes=" + mergeOperationTypes + ")";
	}

	public boolean isMergeOperationTypes() {
		return mergeOperationTypes;
	}
	public void setMergeOperationTypes(boolean mergeOperationTypes) {
		this.mergeOperationTypes = mergeOperationTypes;
	}
}
