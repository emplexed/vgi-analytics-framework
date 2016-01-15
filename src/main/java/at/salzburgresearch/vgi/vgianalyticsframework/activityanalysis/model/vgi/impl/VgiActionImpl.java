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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl;

import java.util.ArrayList;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;

public class VgiActionImpl implements IVgiAction {

	private String actionName = "AC_UNDEFINED";
	private ActionType actionType = ActionType.UNDEFINED;
	private VgiGeometryType geometryType = VgiGeometryType.UNDEFINED;
	private SimpleFeatureType featureType = null;
	private SimpleFeature feature = null;
	
	private ArrayList<VgiActionDefinitionRule> definition = new ArrayList<VgiActionDefinitionRule>();
	private ArrayList<IVgiOperation> operations = new ArrayList<IVgiOperation>();
	
	public enum ActionType {
		CREATE,
		UPDATE,
		DELETE,
		UNDEFINED
	}
	
	/** Constructor */
	public VgiActionImpl() {}
	
	public VgiActionImpl(String actionName, ActionType actionType) {
		this.actionName = actionName;
		this.actionType = actionType;
	}
	
	public VgiActionImpl(IVgiAction action) {
		this.actionName = action.getActionName();
		this.actionType = action.getActionType();
		this.geometryType = action.getGeometryType();
		this.featureType = action.getFeatureType();
		this.feature = action.getFeature();
		this.definition = action.getDefinition();
		this.operations = new ArrayList<IVgiOperation>(action.getOperations());
	}
	
	/** Getter/Setter */
	@Override
	public String getActionName() {
		return actionName;
	}
	@Override
	public void setActionName(String actionName) {
		this.actionName = actionName;
	}
	
	@Override
	public ActionType getActionType() {
		return actionType;
	}
	@Override
	public void setActionType(ActionType actionType) {
		this.actionType = actionType;
	}

	@Override
	public VgiGeometryType getGeometryType() {
		return geometryType;
	}
	@Override
	public void setGeometryType(VgiGeometryType geometryType) {
		this.geometryType = geometryType;
	}

	@Override
	public SimpleFeatureType getFeatureType() {
		return featureType;
	}
	@Override
	public void setFeatureType(SimpleFeatureType featureType) {
		this.featureType = featureType;
	}

	@Override
	public SimpleFeature getFeature() {
		return feature;
	}
	@Override
	public void setFeature(SimpleFeature feature) {
		this.feature = feature;
	}

	@Override
	public ArrayList<VgiActionDefinitionRule> getDefinition() {
		return definition;
	}

	@Override
	public void addDefinitionRule(VgiActionDefinitionRule operation) {
		definition.add(operation);
	}
	
	@Override
	public void addDefinitionRule(ArrayList<VgiActionDefinitionRule> operations) {
		definition.addAll(operations);
	}

	@Override
	public ArrayList<IVgiOperation> getOperations() {
		return operations;
	}
	@Override
	public void addOperation(IVgiOperation operation) {
		this.operations.add(operation);
	}
	
	@Override
	public void addOperations(ArrayList<IVgiOperation> operations) {
		this.operations.addAll(operations);
	}
	
	@Override
	public String toString() {
		if (this.getOperations().size() > 0) {
			return "VgiAction " + this.actionName + " (" + this.getOperations().get(0).getVgiGeometryType() + "/" + this.getOperations().get(0).getOid() + ")";
		}
		return "VgiAction " + this.actionName + " (0 operations)";
	}
}
