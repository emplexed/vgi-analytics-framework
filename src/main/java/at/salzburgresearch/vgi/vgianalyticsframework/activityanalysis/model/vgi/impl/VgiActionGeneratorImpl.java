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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.geotools.geometry.jts.Geometries;
import org.opengis.feature.simple.SimpleFeature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiActionGenerator;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeatureType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiModelFactory;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiActionImpl.ActionType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.impl.FeatureBuilderConsumer;

/**
 * Aggregates VGI operations to VGI actions
 * The method generateActions receives a feature including a set of operations, aggregates those to actions and adds the actions to the feature
 *
 */
public class VgiActionGeneratorImpl implements IVgiActionGenerator {
	private static Logger log = Logger.getLogger(VgiActionGeneratorImpl.class);
	
	private IVgiModelFactory actionFactory = new VgiModelFactoryImpl();
	
	@Autowired
	@Qualifier("vgiPipelineSettings")
	private IVgiPipelineSettings settings;
	
	@Autowired
	@Qualifier("featureBuilderConsumer")
	private FeatureBuilderConsumer geometryAssemblerConsumer;
	
	private Map<String, String> currentFeatureTags = null;
	private List<IVgiFeatureType> previousFeatureType = null;
	
	private IVgiFeature featureToAssemble = null;
	
	/** Constructor */
	public VgiActionGeneratorImpl(IVgiModelFactory actionFactory, IVgiPipelineSettings settings) {
		this.actionFactory = actionFactory;
		this.settings = settings;
	}
	
	/**
	 * Aggregates VGI operations to VGI actions
	 * @param feature
	 */
	@Override
	public void generateActions(IVgiFeature feature) {
		
		/** Initialize variables */
		if (feature.getActionList() == null) {
			feature.setActionList(new ArrayList<IVgiAction>());
		} else {
			log.warn("---------------------- IS THIS POSSIBLE ??? > " + feature.getActionList().size());
		}
		currentFeatureTags = new HashMap<String, String>();
		previousFeatureType = new ArrayList<IVgiFeatureType>();
		
		featureToAssemble = new VgiFeatureImpl();
		featureToAssemble.setOid(feature.getOid());
		featureToAssemble.setVgiGeometryType(feature.getVgiGeometryType());
		
		/** Sort operations in this group by hierarchy level */
		Collections.sort(feature.getOperationList(), VgiOperationImpl.getVersionComparator());
		
		List<IVgiOperation> featureOperations = new ArrayList<IVgiOperation>(feature.getOperationList());
		
		/** Creates aggregation groups and aggregates operations to actions */
		/** repeat until operationList is empty */
		while (!featureOperations.isEmpty()) {
			
			long previousTimestamp = 0l;
			int previousUId = 0;
			
			/** Creates aggregation groups based on timestamp and uid */
			List<IVgiOperation> operationGroup = new ArrayList<IVgiOperation>();
			
			for (IVgiOperation operation : featureOperations) {
				
				long diff = (previousTimestamp != 0l) ? operation.getTimestamp().getTime() - previousTimestamp : 0l;

				/** if group is empty OR (timeDiff < MaxTimeDiff AND same UId) */
				/** if time difference is greater than buffer, operation group is complete  */
				if (diff > settings.getActionTimeBuffer()) break;
				
				/** Add operation of all users to featureToAssemble object which will be used to built a SimpleFeature object later */
				if (!featureToAssemble.getOperationList().contains(operation)) featureToAssemble.getOperationList().add(operation);
				
				/** only add operations of same user to the group */
				if (!operationGroup.isEmpty() && operation.getUid() != previousUId) continue;					
				
				/** Add operation to group */
				operationGroup.add(operation);
				
				/** remember operation timestamp and uid */
				previousTimestamp = operation.getTimestamp().getTime();
				previousUId = operation.getUid();
			}
			
			featureOperations.removeAll(operationGroup);

			/** Determine current feature type */
			List<IVgiFeatureType> featureTypeList = determineFeatureTypes(operationGroup);
			
			/** Sort operations in this group by hierarchy level */
			Collections.sort(operationGroup, Collections.reverseOrder(VgiOperationImpl.getOperationTypeComparator()));
			
			/** Aggregate operations in this group */
			
			/** Aggregate feature types which have been deleted */
			for (IVgiFeatureType iFeatureType : previousFeatureType) {
				
				if (featureTypeList.contains(iFeatureType)) continue;
				aggregate(feature.getActionList(), new ArrayList<IVgiOperation>(operationGroup), ActionType.DELETE, iFeatureType);
			}
			for (IVgiFeatureType iFeatureType : featureTypeList) {
				
				if (previousFeatureType.contains(iFeatureType)) {
					aggregate(feature.getActionList(), new ArrayList<IVgiOperation>(operationGroup), ActionType.UPDATE, iFeatureType);
				} else {
					aggregate(feature.getActionList(), new ArrayList<IVgiOperation>(operationGroup), ActionType.CREATE, iFeatureType);
				}
			}
			
			previousFeatureType = featureTypeList;
		}
	}
	
	/**
	 * Generates actions from operation in operationGroup for the specified feature type
	 * @param actionList actions will be added to this list
	 * @param operationGroup includes one operation group (grouped by time buffer and uid)
	 * @param actionType (Create, Update or Delete)
	 * @param featureType (e.g. streets, buildings)
	 */
	private void aggregate(List<IVgiAction> actionList, List<IVgiOperation> operationGroup, ActionType actionType, IVgiFeatureType featureType) {
		
		SimpleFeature feature = geometryAssemblerConsumer.assembleGeometry(featureToAssemble, featureType);
		/** Geometries without nodes (due to license change) cannot be assembled */
		if (feature == null) return;
		
		/** Remove operations which do not belong to this featureType from operationGroup */
		if (settings.isIgnoreNonFeatureTypeTags()) {
			List<IVgiOperation> operationsToIgnore = new ArrayList<IVgiOperation>();
			
			for (IVgiOperation operation : operationGroup) {
				if (operation.getVgiOperationType().equals(VgiOperationType.OP_ADD_TAG) ||
					operation.getVgiOperationType().equals(VgiOperationType.OP_MODIFY_TAG_VALUE) ||
					operation.getVgiOperationType().equals(VgiOperationType.OP_REMOVE_TAG)) {
//					if (!featureType.getPropertyTags().contains(operation.getKey())) operationsToIgnore.add(operation);
					if (featureType.getFeatureType().getType(operation.getKey()) == null) operationsToIgnore.add(operation); //TODO test
				}
			}
			operationGroup.removeAll(operationsToIgnore);
		}
		
		/** Repeat until operationGroup is empty */
		while (!operationGroup.isEmpty()) {
			
			IVgiAction action = null;
			int actionDefinitionPointer = 0;
			
			/** Find action type */
			action = createAction(operationGroup, actionType, feature);
			if (action == null) { //TODO ???
//				log.info("does this happen???");;
				if (actionType.equals(ActionType.CREATE)) {
					actionType = ActionType.UPDATE;
					continue;
				} else {
					break;
				}
			}
			
			/** Add operations to action */
			operationGroupLoop:
			for (IVgiOperation operation : operationGroup) {
				do {
					/** End of filter list */
					if (actionDefinitionPointer == action.getDefinition().size()) break;
					/** Compare feature id */
					int compareHierarchy = VgiOperationType.getVgiOperationTypeComparator().compare(
							action.getDefinition().get(actionDefinitionPointer).getVgiOperationType(), 
							operation.getVgiOperationType());
					if (compareHierarchy == 0) {
						/** operation type member of action definition > Add this operation to action */
						action.addOperation(operation);
						break;
					} else if (compareHierarchy < 0) {
						/** operation type NO member of action definition > SKIP this operation */
						continue operationGroupLoop;
					}
					/** Current action definition level is too low > NEXT level */
					actionDefinitionPointer++;
				} while (true);
			}
			
			/** Remove aggregated operation from the group */
			operationGroup.removeAll(action.getOperations());
			
			if (action.getOperations().size()==0) {
				log.warn("Action has no operations!");
				break;
			}
			
			action.setFeature(feature);//TODO inefficient
			action.setFeatureType(featureType.getFeatureType());
			actionList.add(action);
		}
		
//		/** Operation group should be empty */
//		if (operationGroup.size() > 0) {
//			log.warn(operationGroup.size() + " operations in this group have NOT been aggregated " + operationGroup.get(0).getVgiGeometryType() + "/" + operationGroup.get(0).getOid());
//		}
	}
	
	private IVgiAction createAction(List<IVgiOperation> operationGroup, ActionType actionType, SimpleFeature feature) {
		IVgiAction action = null;
		
		for (IVgiOperation operation : operationGroup) {
			for (IVgiAction a : settings.getActionDefinitionList()) {
				
				/** Check geometry type */
				if (!a.getGeometryType().equals(VgiGeometryType.UNDEFINED) && (!a.getGeometryType().equals(operation.getVgiGeometryType()))) continue;
				if (!a.getActionType().equals(actionType)) continue;
				
				if (feature.getDefaultGeometry() instanceof Point) {
					if (!Geometries.get((Geometry)feature.getDefaultGeometry()).equals(Geometries.POINT)) continue;
				} else if (feature.getDefaultGeometry() instanceof LineString) {
					if (!Geometries.get((Geometry)feature.getDefaultGeometry()).equals(Geometries.LINESTRING)) continue;
				} else if (feature.getDefaultGeometry() instanceof Polygon) {
					if (!Geometries.get((Geometry)feature.getDefaultGeometry()).equals(Geometries.POLYGON)) continue;
				}
				
				/** Find action */
				for (VgiActionDefinitionRule r : a.getDefinition()) {
					
					//TODO no update action after delete action
					//update operation in same operation group are aggregated to update action
					
					switch (r.getEntryPoint()) {
					case NO:
						continue;
					case YES:
					default:
						break;
					}
					
					if (!r.getVgiOperationType().equals(operation.getVgiOperationType())) continue;
					
					action = actionFactory.newAction(a.getActionName(), actionType);
					action.addDefinitionRule(a.getDefinition());
					action.setActionType(actionType);
					return action;
				}
			}
		}
		
		return null;
	}
	
	/**
	 * Determines the features type of a feature
	 * 
	 * The feature type is based on 
	 *  (1) the setting parameter <code>featureTypeAttribute</code> if it is not empty
	 *  (2) the tag filter if it is not empty
//	 *  ((3) the primary tag list)
	 * 
	 * @param feature operations of a feature
	 * @return List of feature types
	 */
	private List<IVgiFeatureType> determineFeatureTypes(List<IVgiOperation> operationGroup) {
		List<IVgiFeatureType> featureTypes = new ArrayList<IVgiFeatureType>();
			
		refreshCurrentTagList(operationGroup);
		
		/** (2) Determine feature type based on tag filter */
		for (String tagKey : currentFeatureTags.keySet()) {
			
			for (String featureTypeName : settings.getFeatureTypeList().keySet()) {
				IVgiFeatureType featureType = settings.getFeatureTypeList().get(featureTypeName);
				
				if (featureType.getFeatureTypeTags().containsKey(tagKey)) {
					
					if (featureType.getFeatureTypeTags().get(tagKey).size() == 0) {
						/** (2a) Only tag key is used for feature type */
//						if (!featureTypes.containsKey(tagKey)) featureTypes.put(tagKey, new ArrayList<String>());
//						featureTypes.get(tagKey).add("");
						if (!featureTypes.contains(featureType)) featureTypes.add(featureType);
//						if (!featureTypes.containsKey(tagKey)) featureTypes.put(tagKey, "");
						
					} else if (featureType.getFeatureTypeTags().get(tagKey).size() == 1 && featureType.getFeatureTypeTags().get(tagKey).get(0).equals("_value")) {
						/** (2b) All tag values are used for feature type */
						if (!featureTypes.contains(featureType)) featureTypes.add(featureType);
//						if (!featureTypes.contains(featureType)) featureTypes.put(tagKey, new ArrayList<String>());
//						featureTypes.get(tagKey).add(currentFeatureTags.get(tagKey));
//						if (!featureTypes.containsKey(tagKey)) featureTypes.put(tagKey, currentFeatureTags.get(tagKey));
						
					} else if (featureType.getFeatureTypeTags().get(tagKey).contains(currentFeatureTags.get(tagKey))) {
						/** (2c) Selected tag values are used for feature type */
						if (!featureTypes.contains(featureType)) featureTypes.add(featureType);
//						if (!featureTypes.contains(featureType)) featureTypes.put(tagKey, new ArrayList<String>());
//						featureTypes.get(tagKey).add(currentFeatureTags.get(tagKey));
//						if (!featureTypes.containsKey(tagKey)) featureTypes.put(tagKey, currentFeatureTags.get(tagKey));
					}
				}
			}
		}
		
//		if (featureTypes.containsKey("addr:housenumber")) {
//			if (featureTypes.get("addr:housenumber") != null) {
//				if (featureTypes.get("addr:housenumber").contains("interpolation")) {
//					
//				}
//			}
//		}
		
		return featureTypes;
	}
	
	/**
	 * Recreates a list of the current tags based on operations
	 * @param feature VGI feature with operations
	 */
	public void refreshCurrentTagList(List<IVgiOperation> operationList) {
//		boolean isFeatureVisible = false;
		
		/** Find current tags */
		for (IVgiOperation operation : operationList) {
			if (operation.getVgiOperationType().equals(VgiOperationType.OP_CREATE_NODE) 
					|| operation.getVgiOperationType().equals(VgiOperationType.OP_CREATE_WAY)
					|| operation.getVgiOperationType().equals(VgiOperationType.OP_CREATE_RELATION)) {
//				isFeatureVisible = true;
				currentFeatureTags.clear();
			}
			if (operation.getVgiOperationType().equals(VgiOperationType.OP_DELETE_NODE)
					|| operation.getVgiOperationType().equals(VgiOperationType.OP_DELETE_WAY)
					|| operation.getVgiOperationType().equals(VgiOperationType.OP_DELETE_RELATION)) {
//				isFeatureVisible = false;
				currentFeatureTags.clear();
			}
			
			if (operation.getVgiOperationType().equals(VgiOperationType.OP_RECREATE_NODE)
					|| operation.getVgiOperationType().equals(VgiOperationType.OP_RECREATE_WAY)
					|| operation.getVgiOperationType().equals(VgiOperationType.OP_RECREATE_RELATION)) {
//				isFeatureVisible = true;
				currentFeatureTags.clear();
			}
			
			if (operation.getVgiOperationType().equals(VgiOperationType.OP_ADD_TAG)) {
				currentFeatureTags.put(operation.getKey(), operation.getValue());
			} else if (operation.getVgiOperationType().equals(VgiOperationType.OP_MODIFY_TAG_VALUE)) {
				currentFeatureTags.put(operation.getKey(), operation.getValue());
			} else if (operation.getVgiOperationType().equals(VgiOperationType.OP_REMOVE_TAG)) {
//				if (isFeatureVisible) currentFeatureTags.remove(operation.getKey());
				currentFeatureTags.remove(operation.getKey());
			}
		}
	}
}
