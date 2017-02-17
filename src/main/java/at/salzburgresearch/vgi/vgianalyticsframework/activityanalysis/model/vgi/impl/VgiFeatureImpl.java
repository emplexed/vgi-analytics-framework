/** Copyright 2017, Simon Gröchenig, Salzburg Research Forschungsgesellschaft m.b.H.

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
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.vividsolutions.jts.geom.Envelope;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;

public class VgiFeatureImpl extends VgiBaseObjectImpl implements IVgiFeature {
	
	protected List<IVgiOperation> operationList = null;
	protected List<IVgiAction> actionList = null;

	protected Envelope boundingBox = null;
	protected String quadtreePath = "";
	
	protected LocalizeType localizeType = LocalizeType.UNDEFINED;
	
	public enum LocalizeType {
		WITHIN,
		OVERLAPS,
		UNDEFINED
	}
	
	public VgiFeatureImpl() {
		this.operationList = new ArrayList<IVgiOperation>();
	}

	public VgiFeatureImpl(ArrayList<IVgiOperation> operationList) {
		this.operationList = operationList;
	}
	
	@Override
	public String toString() {
		return super.getVgiGeometryType() + " " + super.getOid() + " (" + operationList.size() + " ops)";
	}
	
	/** 
	 * Recreates the set of tags for a specific timestamp. Operations are used.
	 * @param feature VGI feature with operations
	 * @param timestamp The set of tags is build for this time
	 * @return
	 */
	public static Map<String, String> getCurrentTagsFromOperations(IVgiFeature feature, Date timestamp) {
		Map<String, String> tags = new HashMap<String, String>();
		boolean isFeatureVisible = false;

		Collections.sort(feature.getOperationList(), VgiOperationImpl.getVersionComparator());
		/** Find current tags */
		for (IVgiOperation operation : feature.getOperationList()) {
			if (operation.getVgiOperationType().equals(VgiOperationType.OP_CREATE_NODE) 
					|| operation.getVgiOperationType().equals(VgiOperationType.OP_CREATE_WAY)
					|| operation.getVgiOperationType().equals(VgiOperationType.OP_CREATE_RELATION)) {
				isFeatureVisible = true;
				tags.clear();
			}
			if (operation.getVgiOperationType().equals(VgiOperationType.OP_DELETE_NODE)) isFeatureVisible = false;
			if (operation.getVgiOperationType().equals(VgiOperationType.OP_DELETE_WAY)) isFeatureVisible = false;
			if (operation.getVgiOperationType().equals(VgiOperationType.OP_DELETE_RELATION)) isFeatureVisible = false;
			
			if (operation.getVgiOperationType().equals(VgiOperationType.OP_RECREATE_NODE)) isFeatureVisible = true;
			if (operation.getVgiOperationType().equals(VgiOperationType.OP_RECREATE_WAY)) isFeatureVisible = true;
			if (operation.getVgiOperationType().equals(VgiOperationType.OP_RECREATE_RELATION)) isFeatureVisible = true;
			
			if (timestamp != null && operation.getTimestamp().after(timestamp)) break;
			
			if (operation.getVgiOperationType().equals(VgiOperationType.OP_ADD_TAG)) {
				tags.put(operation.getKey(), operation.getValue());
			} else if (operation.getVgiOperationType().equals(VgiOperationType.OP_MODIFY_TAG_VALUE)) {
				tags.put(operation.getKey(), operation.getValue());
			} else if (operation.getVgiOperationType().equals(VgiOperationType.OP_REMOVE_TAG)) {
				if (isFeatureVisible) tags.remove(operation.getKey());
			}
		}
		
		return tags;
	}
	/** 
	 * Recreates the set of tags for a specific timestamp. Actions are used.
	 * @param feature VGI feature with operations
	 * @param timestamp The set of tags is build for this time
	 * @return
	 */
	public static Map<String, String> getCurrentTagsFromActions(IVgiFeature feature, Date timestamp) {
		Map<String, String> tags = new HashMap<String, String>();
		boolean isFeatureVisible = false;

		for (IVgiAction action : feature.getActionList()) {
			
			Collections.sort(action.getOperations(), VgiOperationImpl.getVersionComparator());
			/** Find current tags */
			for (IVgiOperation operation : action.getOperations()) {
				if (operation.getVgiOperationType().equals(VgiOperationType.OP_CREATE_NODE) 
						|| operation.getVgiOperationType().equals(VgiOperationType.OP_CREATE_WAY)
						|| operation.getVgiOperationType().equals(VgiOperationType.OP_CREATE_RELATION)) {
					isFeatureVisible = true;
					tags.clear();
				}
				if (operation.getVgiOperationType().equals(VgiOperationType.OP_DELETE_NODE)) isFeatureVisible = false;
				if (operation.getVgiOperationType().equals(VgiOperationType.OP_DELETE_WAY)) isFeatureVisible = false;
				if (operation.getVgiOperationType().equals(VgiOperationType.OP_DELETE_RELATION)) isFeatureVisible = false;
				
				if (operation.getVgiOperationType().equals(VgiOperationType.OP_RECREATE_NODE)) isFeatureVisible = true;
				if (operation.getVgiOperationType().equals(VgiOperationType.OP_RECREATE_WAY)) isFeatureVisible = true;
				if (operation.getVgiOperationType().equals(VgiOperationType.OP_RECREATE_RELATION)) isFeatureVisible = true;
				
				if (timestamp != null && operation.getTimestamp().after(timestamp)) break;
				
				if (operation.getVgiOperationType().equals(VgiOperationType.OP_ADD_TAG)) {
					tags.put(operation.getKey(), operation.getValue());
				} else if (operation.getVgiOperationType().equals(VgiOperationType.OP_MODIFY_TAG_VALUE)) {
					tags.put(operation.getKey(), operation.getValue());
				} else if (operation.getVgiOperationType().equals(VgiOperationType.OP_REMOVE_TAG)) {
					if (isFeatureVisible) tags.remove(operation.getKey());
				}
			}
		}
		
		return tags;
	}
	
	public static Map<String, List<String>> getAllTagsFromOperations(IVgiFeature feature) {
		Map<String, List<String>> tags = new HashMap<String, List<String>>();
		
		Collections.sort(feature.getOperationList(), VgiOperationImpl.getVersionComparator());
		/** Find current tags */
		for (IVgiOperation operation : feature.getOperationList()) {
			if (operation.getVgiOperationType().equals(VgiOperationType.OP_ADD_TAG)) {
				if (!tags.containsKey(operation.getKey())) tags.put(operation.getKey(), new ArrayList<String>());
				tags.get(operation.getKey()).add(operation.getValue());
				
			} else if (operation.getVgiOperationType().equals(VgiOperationType.OP_MODIFY_TAG_VALUE)) {
				if (!tags.containsKey(operation.getKey())) tags.put(operation.getKey(), new ArrayList<String>());
				tags.get(operation.getKey()).add(operation.getValue());
			}
		}
		
		return tags;
	}
	
	/**
	 * Check whether this feature contains one of the tags specified in filterTag parameter
	 * @param filter tag list
	 * @return true if feature contains tag mentioned in tag filter
	 */
	public boolean filterByTag(Map<String, List<String>> filterTag) {
		/** if no tag is defined, return true */
		if (filterTag.keySet().size() == 0) return true;
		
		/** Filter feature by tag */
		Map<String, List<String>> tags = VgiFeatureImpl.getAllTagsFromOperations(this);
		
		for (String tagKey : tags.keySet()) {
			
			/** Does filter list contain the key of this operation? */
			if (filterTag.containsKey(tagKey)) {
				
				/** Return true if filter list contains the value of this operation */
				/** Compare only tag keys */
				if (filterTag.get(tagKey).size() == 0) return true;
				
				/** Compare keys and values */
				tags.get(tagKey).retainAll(filterTag.get(tagKey));
				if (tags.get(tagKey).size() > 0) return true;
			}
		}
		/** Otherwise return false */
		return false;
	}
	
    public static Comparator<IVgiFeature> getFeatureComparator() {
        return new Comparator<IVgiFeature>() {
    		public int compare(IVgiFeature f1, IVgiFeature f2) {
    			/** Compare two features: (1) geometry type, (2) oid */
    			if (f1.getVgiGeometryType().equals(f2.getVgiGeometryType())) {
    				if (f1.getOid() == f2.getOid()) {
        	            return 0;
        			} else if (f1.getOid() > f2.getOid()) {
        	            return 1;
        			} else {
        	            return -1;
        			}
    			} else if (f1.getVgiGeometryType().ordinal() > f2.getVgiGeometryType().ordinal()) {
    	            return 1;
    			} else {
    	            return -1;
    			}
    		}
        };
    }
	
	@Override
	public List<IVgiOperation> getOperationList() {
		return operationList;
	}
	@Override
	public void setOperationList(List<IVgiOperation> operationList) {
		this.operationList = operationList;
	}
	@Override
	public void addOperation(IVgiOperation operation) {
		this.operationList.add(operation);
	}

	@Override
	public List<IVgiAction> getActionList() {
		return actionList;
	}
	@Override
	public void setActionList(List<IVgiAction> actionList) {
		this.actionList = actionList;
	}
	@Override
	public void addAction(IVgiAction action) {
		this.actionList.add(action);
	}

	@Override
	public Envelope getBBox() {
		return boundingBox;
	}
	@Override
	public void setBBox(Envelope bbox) {
		this.boundingBox = bbox;
	}

	@Override
	public String getQuadtreePath() {
		return quadtreePath;
	}
	@Override
	public void setQuadtreePath(String quadtreePath) {
		this.quadtreePath = quadtreePath;
	}

	@Override
	public LocalizeType getLocalizeType() {
		return localizeType;
	}
	@Override
	public void setLocalizeType(LocalizeType localizeType) {
		this.localizeType = localizeType;
	}
}
