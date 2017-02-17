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

package at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Way;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiModelFactory;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.IOsmVgiWayOperationGenerator;

public class VgiOperationGeneratorOsmWayImpl extends AbstractVgiOperationOsmGenerator implements IOsmVgiWayOperationGenerator {
	private static Logger log = Logger.getLogger(VgiOperationGeneratorOsmWayImpl.class);

	public VgiOperationGeneratorOsmWayImpl(IVgiModelFactory operationFactory) {
		super(operationFactory);
	}
	
	/**
	 * Extracts VGI Operations from geographic data model. in this implementation OSM model is used as input
	 * 
	 * if no operations are found an empty list is returned (null save)
	 * 
	 * @param value current version of the way
	 * @param oldValue previous version of the way
	 */
	@Override
	public List<IVgiOperation> generateWayOperations(Way value, Way oldValue) {
		
		List<IVgiOperation> operationList = new ArrayList<IVgiOperation>();
		
		if (oldValue == null) {
			operationList.add(operationFactory.newOperation(value.getId(), VgiGeometryType.LINE, VgiOperationType.OP_CREATE_WAY, value.getUid(), value.getUsername(), value.getTimestamp(), (short)value.getVersion(), (int)value.getChangesetId(), null, "", "", -1, -1));
		}
		
		List<Long> valueNodeRefs = (value != null) ? value.getWayNodes() : new ArrayList<Long>();
		List<Long> oldValueNodeRefs = (oldValue != null) ? oldValue.getWayNodes() : new ArrayList<Long>();

		List<Long> currentNodeRefs = new ArrayList<Long>(oldValueNodeRefs);
		
		/** PHASE A: Find OpRemoveNode operations */
		List<Long> availableNodeRefs = new ArrayList<Long>(valueNodeRefs);
		for (int i=oldValueNodeRefs.size()-1; i>=0; i--) {
			if (!availableNodeRefs.contains(oldValueNodeRefs.get(i))) {
				operationList.add(operationFactory.newOperation(value.getId(), VgiGeometryType.LINE, VgiOperationType.OP_REMOVE_NODE, value.getUid(), value.getUsername(), value.getTimestamp(), (short)value.getVersion(), (int)value.getChangesetId(), null, "", "", oldValueNodeRefs.get(i), i));
				/** Remove node from current node list */
				currentNodeRefs.remove(i);
			} else {
				/** Remove node from available node list */
				availableNodeRefs.remove(oldValueNodeRefs.get(i));
			}
		}
		
		/** PHASE B: Find OpAddNode operations */
		for (int i=0; i<valueNodeRefs.size(); i++) {
			if (availableNodeRefs.contains(valueNodeRefs.get(i))) {
				operationList.add(operationFactory.newOperation(value.getId(), VgiGeometryType.LINE, VgiOperationType.OP_ADD_NODE, value.getUid(), value.getUsername(), value.getTimestamp(), (short)value.getVersion(), (int)value.getChangesetId(), null, "", "", valueNodeRefs.get(i), i));
				/** Adds node to current node list */
				currentNodeRefs.add(i, valueNodeRefs.get(i));
				availableNodeRefs.remove(valueNodeRefs.get(i));
			}
		}
		
		if (currentNodeRefs.size() != valueNodeRefs.size()) {
			log.error("Current Node Ref List has " + currentNodeRefs.size() + " items, while Value Node Ref List has " + valueNodeRefs.size() + " items (way/" + value.getId() + "/v" + value.getVersion() + ")");
		}
		
		/** PHASE C: Find OpReorderNode operations */
		for (int position = 0; position < currentNodeRefs.size(); position++) {
			if (!currentNodeRefs.get(position).equals(valueNodeRefs.get(position))) {
				/** Find next valueNodeRef in remaining currentNodeRefs */
				for (int i=position+1; i<currentNodeRefs.size(); i++) {
					if (currentNodeRefs.get(i).equals(valueNodeRefs.get(position))) {
						operationList.add(operationFactory.newOperation(value.getId(), VgiGeometryType.LINE, VgiOperationType.OP_REORDER_NODE, value.getUid(), value.getUsername(), value.getTimestamp(), (short)value.getVersion(), (int)value.getChangesetId(), null, "", "", i, position));
						/** Reorder node within node list */
						currentNodeRefs.remove(i);
						currentNodeRefs.add(position, valueNodeRefs.get(position));
						break;
					}
				}
			}
		}
		
		return operationList;
	}
}
