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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.opengis.feature.simple.SimpleFeatureType;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

public class VgiAnalysisGeometryType extends VgiAnalysisParent implements IVgiAnalysisAction {

	private Map<VgiGeometryType, Map<SimpleFeatureType, Double>> featureByGeometryType = new ConcurrentHashMap<VgiGeometryType, Map<SimpleFeatureType, Double>>();
	
	private List<SimpleFeatureType> featureTypes = new ArrayList<SimpleFeatureType>();

	/** Constructor */
	public VgiAnalysisGeometryType() {
		/** Initialize featureByGeometryType array */
		for (VgiGeometryType type : VgiGeometryType.values()) {
			featureByGeometryType.put(type, new ConcurrentHashMap<SimpleFeatureType, Double>());
		}
	}
	
	@Override
	public void analyze(IVgiAction action, Date timePeriod) {
		Map<SimpleFeatureType, Double> actionsPerGeometryType = featureByGeometryType.get(action.getOperations().get(0).getVgiGeometryType());
		
		SimpleFeatureType featureType = action.getFeatureType();
		
		if (!featureTypes.contains(featureType)) featureTypes.add(featureType);
		
		Double value = actionsPerGeometryType.get(featureType);
		if (value != null) {
			value = value + 1.0;
		} else {
			value = new Double(1.0);
		}
		actionsPerGeometryType.put(featureType, value);
	}
	
	@Override
	public void write(File path) {
		CSVFileWriter writer = new CSVFileWriter(path + "/features_per_geometry_type.csv");
		/** write header */
		String line = "";
		for (SimpleFeatureType tag : featureTypes) {
			line += tag.getName()+";";
		}
		writer.writeLine("geometry_type;"+line);
		/** iterate through rows*/
		for (Map.Entry<VgiGeometryType, Map<SimpleFeatureType,Double>> featureType : featureByGeometryType.entrySet()) {
			/** write row values */
			Map<SimpleFeatureType, Double> m = featureType.getValue();

			double sum = 0.0;
			line = "";
			for (SimpleFeatureType tag : featureTypes) {
				line += (m.containsKey(tag)) ? decimalFormat.format(m.get(tag)) : "";
				line += ";";
				sum += (m.containsKey(tag)) ? m.get(tag) : 0;
			}
			
			if (sum > 0) writer.writeLine(featureType.getKey() + ";" + line);
		}
		writer.closeFile();
	}

	@Override
	public void reset() {
		featureByGeometryType.clear();
		featureTypes.clear();
	}
	
	@Override
	public String toString() {
		return "VgiAnalysisGeometryType";
	}
}
