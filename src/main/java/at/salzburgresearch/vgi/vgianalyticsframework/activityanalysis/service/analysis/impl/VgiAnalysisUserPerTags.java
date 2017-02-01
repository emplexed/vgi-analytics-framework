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

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

/**
 * Analysis which determines the number of tags per contributor and action type
 *
 */
public class VgiAnalysisUserPerTags extends VgiAnalysisParent implements IVgiAnalysisOperation {
	
	private String tagKey = "";

	/** Constructor */
	public VgiAnalysisUserPerTags(IVgiPipelineSettings settings) {
		this.settings = settings;
	}

	@Override
	public void analyze(IVgiOperation operation, Date timePeriod) {

		VgiAnalysisUser user = findUser(operation.getUid(), operation.getUser());
		
		/** Add, Updated OR removed tag */
		Map<String, long[]> tagByKey = null;
		if (operation.getVgiOperationType().equals(VgiOperationType.OP_ADD_TAG)) {
			tagByKey = user.addedTagPerKey;
		} else if (operation.getVgiOperationType().equals(VgiOperationType.OP_MODIFY_TAG_VALUE)) {
			tagByKey = user.modifiedTagPerKey;
		} else if (operation.getVgiOperationType().equals(VgiOperationType.OP_REMOVE_TAG)) {
			tagByKey = user.removedTagPerKey;
		} else {
			return;
		}
		
		/** read key (or value) */
		String tag = operation.getKey();
		if (!tagKey.equals("")) {
			if (!operation.getKey().equals(tagKey)) return;
			tag = operation.getValue();
		}
		
		int geomType = 0;
		if (operation.getVgiGeometryType().equals(VgiGeometryType.LINE)) {
			geomType = 1;
		} else if (operation.getVgiGeometryType().equals(VgiGeometryType.RELATION)) {
			geomType = 2;
		}
		
		if (!tagByKey.containsKey(tag)) {
			long[] d = {0,0,0};
			tagByKey.put(tag, d);
		}
		tagByKey.get(tag)[geomType]++;
	}
	
	@Override
	public void write(File path) {
		try (
				CSVFileWriter writerAdd = new CSVFileWriter(path + "/added_tag_per_key.csv");
				CSVFileWriter writerUpdate = new CSVFileWriter(path + "/modified_tag_per_key.csv");
				CSVFileWriter writerRemove = new CSVFileWriter(path + "/removed_tag_per_key.csv");) {
			/** write header */
			if (tagKey.equals("")) {
				writerAdd.writeLine("uid;tag_key;count(n);count(w);count(r)");
				writerUpdate.writeLine("uid;tag_key;count(n);count(w);count(r)");
				writerRemove.writeLine("uid;tag_key;count(n);count(w);count(r)");
			} else {
				writerAdd.writeLine("uid;tag_value ("+tagKey+");count(n);count(w);count(r)");
				writerUpdate.writeLine("uid;tag_value ("+tagKey+");count(n);count(w);count(r)");
				writerRemove.writeLine("uid;tag_value ("+tagKey+");count(n);count(w);count(r)");
			}
			/** iterate through rows*/
			for (int user : userAnalysis.keySet()) {
				VgiAnalysisUser u = userAnalysis.get(user);
				
				/** write row values */
				for (String type : u.addedTagPerKey.keySet()) {
					if (u.addedTagPerKey.get(type)[0] + u.addedTagPerKey.get(type)[1] + u.addedTagPerKey.get(type)[2] > 0) {
						/** remove line breaks because of node 456999774 (amenity &#13;) */
						writerAdd.writeLine(u.getUid() + ";" + type.replace(";", ",").replace("\n", " ").replace("\r", " ") + ";" + ((u.addedTagPerKey.get(type)[0] != 0) ? u.addedTagPerKey.get(type)[0] : "") + ";" + ((u.addedTagPerKey.get(type)[1] != 0) ? u.addedTagPerKey.get(type)[1] : "") + ";" + ((u.addedTagPerKey.get(type)[2] != 0) ? u.addedTagPerKey.get(type)[2] : ""));
					}
				}
				
				/** write row values */
				for (String type : u.modifiedTagPerKey.keySet()) {
					if (u.modifiedTagPerKey.get(type)[0] + u.modifiedTagPerKey.get(type)[1] + u.modifiedTagPerKey.get(type)[2] > 0) {
						writerUpdate.writeLine(u.getUid() + ";" + type.replace(";", ",").replace("\n", " ").replace("\r", " ") + ";" + ((u.modifiedTagPerKey.get(type)[0] != 0) ? u.modifiedTagPerKey.get(type)[0] : "") + ";" + ((u.modifiedTagPerKey.get(type)[1] != 0) ? u.modifiedTagPerKey.get(type)[1] : "") + ";" + ((u.modifiedTagPerKey.get(type)[2] != 0) ? u.modifiedTagPerKey.get(type)[2] : ""));
					}
				}
				
				/** write row values */
				for (String type : u.removedTagPerKey.keySet()) {
					if (u.removedTagPerKey.get(type)[0] + u.removedTagPerKey.get(type)[1] + u.removedTagPerKey.get(type)[2] > 0) {
						writerRemove.writeLine(u.getUid() + ";" + type.replace(";", ",").replace("\n", " ").replace("\r", " ") + ";" + ((u.removedTagPerKey.get(type)[0] != 0) ? u.removedTagPerKey.get(type)[0] : "") + ";" + ((u.removedTagPerKey.get(type)[1] != 0) ? u.removedTagPerKey.get(type)[1] : "") + ";" + ((u.removedTagPerKey.get(type)[2] != 0) ? u.removedTagPerKey.get(type)[2] : ""));
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
		return this.getClass().getSimpleName() + " (tagKey=" + tagKey + ")";
	}

	public String getTagKey() {
		return tagKey;
	}

	public void setTagKey(String tagKey) {
		this.tagKey = tagKey;
	}
}
