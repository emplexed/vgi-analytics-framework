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

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

/**
 * Writes all action and operation details into the file.
 *
 */
public class VgiAnalysisActionDetails extends VgiAnalysisParent implements IVgiAnalysisAction {
	
	private boolean includeOperationDetails = false;

	private List<String> actionListWriter = new ArrayList<String>();
	private long actionListWriterCount = 0l;

	/** Constructor */
	public VgiAnalysisActionDetails(IVgiPipelineSettings settings) {
		this.settings = settings;
	}

	@Override
	public void analyze(IVgiAction action, Date timePeriod) {
		actionListWriterCount++;
		if (this.includeOperationDetails) {
			for (IVgiOperation operation : action.getOperations()) {
				actionListWriter.add(actionListWriterCount + ";" + operation.getOid() + ";" + operation.getVgiGeometryType() + ";" + action.getFeatureType().getName() + ";" + action.getActionName() + ";" + operation.getVgiOperationType() + ";" + operation.getUid() + ";" + operation.getVersion() + ";" + dateTimeFormat.format(operation.getTimestamp()) + ";" + operation.getChangesetid() + ";\"" + operation.getCoordinate() + "\";\"" + operation.getKey() + "\";\"" + operation.getValue() + "\";\"" + operation.getRefId() + "\";\"" + operation.getPosition() + "\";");
			}
		} else {
			IVgiOperation operation = action.getOperations().get(0);
			actionListWriter.add(actionListWriterCount + ";" + operation.getOid() + ";" + operation.getVgiGeometryType() + ";" + action.getFeatureType().getName() + ";" + action.getActionName() + ";" + VgiOperationType.OP_UNDEFINED + ";" + operation.getUid() + ";" + operation.getVersion() + ";" + dateTimeFormat.format(operation.getTimestamp()) + ";" + operation.getChangesetid() + ";;;;;;");
		}
	}
	
	@Override
	public void write(File path) {
		try (CSVFileWriter writer = new CSVFileWriter(path + "/action_details.csv")) {
			/** write header */
			writer.writeLine("action_id;oid;geometry_type;feature_type;action_type;operation_type;uid;version;timestamp;changeset;coordinate;key;value;ref_id;position;");
			/** iterate through rows*/
			for (String action : actionListWriter) {
				/** write row values */
				writer.writeLine(action);
			}
		} catch (IOException e) {
			log.error("Error while writing CSV file", e);
		}
	}

	@Override
	public void reset() {
		actionListWriter.clear();
		actionListWriterCount = 0l;
	}
	
	@Override
	public String toString() {
		return "VgiAnalysisActionDetails (includeOperationDetails=" + includeOperationDetails + ")";
	}

	public boolean isIncludeOperationDetails() {
		return includeOperationDetails;
	}
	public void setIncludeOperationDetails(boolean includeOperationDetails) {
		this.includeOperationDetails = includeOperationDetails;
	}

}
