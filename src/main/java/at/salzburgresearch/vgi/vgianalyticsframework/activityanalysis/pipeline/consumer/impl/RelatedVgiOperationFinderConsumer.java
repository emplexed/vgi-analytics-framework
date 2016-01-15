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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.IVgiPipelineConsumer;

public class RelatedVgiOperationFinderConsumer implements IVgiPipelineConsumer {
	
	@Autowired
	ApplicationContext ctx;
	
	private Map<Long, List<IVgiOperation>> childElementList = null;
	private List<IVgiFeature> featureList = null;
	
	/**
	 * Constructor
	 *  */
	public RelatedVgiOperationFinderConsumer() {}
	
	@Override
	public void doBeforeFirstBatch() {}
	
	/**
	 * Finds related operations (OP_UPDATE_COORDINATES for way nodes) and adds them to operation list.
	 * @param batch of operations
	 */
	@Override
	public void handleBatch(List<IVgiFeature> batch) {
		
		/** Iterate through batch, identify possible related operations and add those to child elements */
		for (IVgiFeature batchFeature : batch) {
			for (IVgiOperation batchOperation : batchFeature.getOperationList()) {
				
				if (!batchOperation.getVgiOperationType().equals(VgiOperationType.OP_ADD_NODE) &&
						!batchOperation.getVgiOperationType().equals(VgiOperationType.OP_REMOVE_NODE)) {
					continue;
				}
				
				/** is this refId id a child element in this batch */
				if (childElementList.containsKey(batchOperation.getRefId())) {
					List<IVgiOperation> childElementRelatedOperations = childElementList.get(batchOperation.getRefId());
					if (childElementRelatedOperations == null) {
						childElementRelatedOperations = new ArrayList<IVgiOperation>();
						childElementList.put(batchOperation.getRefId(), childElementRelatedOperations);
					}		
					childElementRelatedOperations.add(batchOperation);
				}
			}
		}
		
		/** Add related operations to features (Ways) */
		
		/** For each feature */
		for (IVgiFeature feature : featureList) {
			
			/** only process line features */
			if (!feature.getVgiGeometryType().equals(VgiGeometryType.LINE)) continue;
			
			/** Node history of the way */
			Map<Long, List<IVgiOperation>> refElementOperationMap = new HashMap<Long, List<IVgiOperation>>();
			for (IVgiOperation operation : feature.getOperationList()) {
				/** Only added and removed nodes are relevant */
				if ((!operation.getVgiOperationType().equals(VgiOperationType.OP_ADD_NODE)) && 
						(!operation.getVgiOperationType().equals(VgiOperationType.OP_REMOVE_NODE))) continue;
				/** Collect operations */
				if (!refElementOperationMap.containsKey(operation.getRefId())) {
					refElementOperationMap.put(operation.getRefId(), new ArrayList<IVgiOperation>());
				}
				refElementOperationMap.get(operation.getRefId()).add(operation);
			}
			if (refElementOperationMap.size() == 0) continue;
			
			/** Prepare list for new operations */
			List<IVgiOperation> newOperations = new ArrayList<IVgiOperation>();
			
			/** For each child element (way node) */
			for (Long refElementId : refElementOperationMap.keySet()) {
				
				/** Get related operations and sort them by timestamp */
				List<IVgiOperation> relatedRefElementOperations = childElementList.get(refElementId);
				if (relatedRefElementOperations == null) continue;
				
				List<IVgiOperation> removeFromRelatedRefElementOperations = new ArrayList<IVgiOperation>();
				
				/** Add operation to feature if node is in node list at this timestamp */
				for (IVgiOperation relatedRefElementOperation : relatedRefElementOperations) {
					
					/** wayNodeHistoryOperation = OP_ADD_NODE OR OP_REMOVE_NODE */
					for (IVgiOperation refElementOperation : refElementOperationMap.get(refElementId)) {
						
						if (refElementOperation.getChangesetid() == relatedRefElementOperation.getChangesetid()) {
							
							if (refElementOperation.getVgiOperationType().equals(VgiOperationType.OP_ADD_NODE)) {
								if (!relatedRefElementOperation.getVgiOperationType().equals(VgiOperationType.OP_REMOVE_NODE)) continue;
								
								/** OP_SPLIT_WAY */
								
								for (IVgiOperation op : feature.getOperationList()) {
									if (!op.getVgiOperationType().equals(VgiOperationType.OP_CREATE_WAY)) continue;
									
									if (op.getChangesetid() == refElementOperation.getChangesetid()) {
										IVgiOperation newOperation = new VgiOperationImpl();
										newOperation.setOid(refElementOperation.getOid());
										newOperation.setVgiGeometryType(VgiGeometryType.LINE);
										newOperation.setVgiOperationType(VgiOperationType.OP_SPLIT_WAY);
										newOperation.setUid(refElementOperation.getUid());
										newOperation.setChangesetid(refElementOperation.getChangesetid());
										newOperation.setTimestamp(refElementOperation.getTimestamp());
										newOperation.setVersion(refElementOperation.getVersion());
										newOperation.setPosition(refElementOperation.getPosition());
										
										newOperations.add(newOperation);
										removeFromRelatedRefElementOperations.add(relatedRefElementOperation);
									}
									break;
								}
								
							} else if (refElementOperation.getVgiOperationType().equals(VgiOperationType.OP_REMOVE_NODE)) {
								if (!relatedRefElementOperation.getVgiOperationType().equals(VgiOperationType.OP_ADD_NODE)) continue;
								
								/** OP_MERGE_WAY */
								
								for (IVgiOperation op : feature.getOperationList()) {
									if (!op.getVgiOperationType().equals(VgiOperationType.OP_DELETE_WAY)) continue;
									
									if (op.getChangesetid() == refElementOperation.getChangesetid()) {
										IVgiOperation newOperation = new VgiOperationImpl();
										newOperation.setOid(op.getOid());
										newOperation.setVgiGeometryType(VgiGeometryType.LINE);
										newOperation.setVgiOperationType(VgiOperationType.OP_MERGE_WAY);
										newOperation.setUid(op.getUid());
										newOperation.setChangesetid(op.getChangesetid());
										newOperation.setTimestamp(op.getTimestamp());
										newOperation.setVersion(op.getVersion());
										newOperation.setPosition(refElementOperation.getPosition());

										newOperations.add(newOperation);
										removeFromRelatedRefElementOperations.add(relatedRefElementOperation);
									}
									break;
								}
							}
						}
					}
				}
				
				relatedRefElementOperations.removeAll(removeFromRelatedRefElementOperations);
			}
			
			/** Add operations */
			feature.getOperationList().addAll(newOperations);
		}
	}
	
	/** relatedFeatureList */
	public Map<Long, List<IVgiOperation>> getChildElementList() {
		return childElementList;
	}
	public void setChildElementList(Map<Long, List<IVgiOperation>> childElementList) {
		this.childElementList = childElementList;
	}
	/** featureList */
	public List<IVgiFeature> getFeatureList() {
		return featureList;
	}
	public void setFeatureList(List<IVgiFeature> featureList) {
		this.featureList = featureList;
	}

	@Override
	public void doAfterLastBatch() { }
}
