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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi;

import java.util.ArrayList;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiActionDefinitionRule;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiActionImpl.ActionType;

/**
 * Interface for VGI actions
 *
 */
public interface IVgiAction {

	/** Action Name */
	String getActionName();
	void setActionName(String actionType);

	/** Action Type */
	ActionType getActionType();
	void setActionType(ActionType actionType);

	/** Geometry Type */
	VgiGeometryType getGeometryType();
	void setGeometryType(VgiGeometryType geometryType);
	
	/** Feature Type */
	SimpleFeatureType getFeatureType();
	void setFeatureType(SimpleFeatureType featureType);
	
	/** SimpleFeature */
	SimpleFeature getFeature();
	void setFeature(SimpleFeature feature);

	/** Action Definition */
	ArrayList<VgiActionDefinitionRule> getDefinition();
	void addDefinitionRule(VgiActionDefinitionRule operation);
	void addDefinitionRule(ArrayList<VgiActionDefinitionRule> operations);

	/** Operation List */
	ArrayList<IVgiOperation> getOperations();
	void addOperation(IVgiOperation operation);
	void addOperations(ArrayList<IVgiOperation> operations);
}
