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

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

/**
 * Analysis which determines the added, updated and removed tags
 *
 */
public class VgiAnalysisTags extends VgiAnalysisParent implements IVgiAnalysisOperation {
	
	private String tagKey = "";
	
	private Map<Date, Map<String, Map<VgiOperationType, Long>>> data = new HashMap<Date, Map<String, Map<VgiOperationType, Long>>>();
	
	/** Constructor */
	public VgiAnalysisTags(IVgiPipelineSettings settings) {
		this.settings = settings;
	}
	
	@Override
	public void analyze(IVgiOperation operation, Date timePeriod) {
		
		if (!data.containsKey(timePeriod)) data.put(timePeriod, new HashMap<String, Map<VgiOperationType, Long>>());
		Map<String, Map<VgiOperationType, Long>> dataTimePeriod = data.get(timePeriod);
		
		/** read key (or value) */
		String tag = operation.getKey();
		if (!tagKey.equals("")) {
			if (!operation.getKey().equals(tagKey)) return;
			tag = operation.getValue();
		}

		if (!dataTimePeriod.containsKey(tag)) dataTimePeriod.put(tag, new HashMap<VgiOperationType, Long>());
		Map<VgiOperationType, Long> dataTag = dataTimePeriod.get(tag);
		
		switch (operation.getVgiOperationType()) {
		case OP_ADD_TAG:
		case OP_MODIFY_TAG_VALUE:
		case OP_REMOVE_TAG:
			if (!dataTag.containsKey(operation.getVgiOperationType())) {
				dataTag.put(operation.getVgiOperationType(), new Long(0));
			}
			break;
		default:
			return;
		}
		
		dataTag.put(operation.getVgiOperationType(), dataTag.get(operation.getVgiOperationType()).longValue() + 1);
	}
	
	@Override
	public void write(File path) {
		try (CSVFileWriter writer = new CSVFileWriter(path + "/tags.csv")) {
			/** write header */
			writer.writeLine("time_period;tag_key;OP_ADD_TAG;OP_MODIFY_TAG_VALUE;OP_REMOVE_TAG");
	
			/** iterate through rows */
			for (Entry<Date, Map<String, Map<VgiOperationType, Long>>> entryTimePeriod : data.entrySet()) {
				for (Entry<String, Map<VgiOperationType, Long>> entryTag : entryTimePeriod.getValue().entrySet()) {
					String line = dateFormat.format(entryTimePeriod.getKey()) + ";" + entryTag.getKey() + ";"
							+ entryTag.getValue().get(VgiOperationType.OP_ADD_TAG) + ";"
							+ entryTag.getValue().get(VgiOperationType.OP_MODIFY_TAG_VALUE) + ";"
							+ entryTag.getValue().get(VgiOperationType.OP_REMOVE_TAG);
					writer.writeLine(line);
				}
			}
		} catch (IOException e) {
			log.error("Error while writing CSV file", e);
		}
	}

	@Override
	public void reset() {
		data = new HashMap<Date, Map<String, Map<VgiOperationType, Long>>>();
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + " (tagKey=" + tagKey + ")";
	}

	public String getTagKey() {
		return tagKey;
	}

	public void setTagKey(String tagKey) {
		this.tagKey = tagKey;
	}
}
