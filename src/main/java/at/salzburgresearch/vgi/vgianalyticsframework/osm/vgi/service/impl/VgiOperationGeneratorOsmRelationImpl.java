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

package at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.Logger;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.OsmElementType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Relation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.RelationMember;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiModelFactory;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.IOsmVgiRelationOperationGenerator;

public class VgiOperationGeneratorOsmRelationImpl extends AbstractVgiOperationOsmGenerator implements IOsmVgiRelationOperationGenerator {
	private static Logger log = Logger.getLogger(VgiOperationGeneratorOsmRelationImpl.class);

	public VgiOperationGeneratorOsmRelationImpl(IVgiModelFactory operationFactory) {
		super(operationFactory);
	}
	
	/**
	 * Extracts VGI Operations from geographic data model. in this implementation OSM model is used as input
	 * 
	 * if no operations are found an empty list is returned (null save)
	 * 
	 * @param value current version of the relation
	 * @param oldValue previous version of the relation
	 */
	@Override
	public List<IVgiOperation> generateRelationOperations(Relation value, Relation oldValue) {
				
		List<IVgiOperation> operationList = new ArrayList<IVgiOperation>();
		
		if (oldValue == null) {
			operationList.add(operationFactory.newOperation(value.getId(), VgiGeometryType.RELATION, VgiOperationType.OP_CREATE_RELATION, value.getUid(), value.getUsername(), value.getTimestamp(), (short)value.getVersion(), (int)value.getChangesetId(), null, "", "", -1, -1));
		}
		
		List<RelationMember> valueMembers = (value != null) ? value.getMembers() : new ArrayList<RelationMember>();
		List<RelationMember> oldValueMembers = (oldValue != null) ? oldValue.getMembers() : new ArrayList<RelationMember>();

		List<RelationMember> currentMembers = new ArrayList<RelationMember>(oldValueMembers);
		
		/** PHASE A: Find OpRemoveNode operations */
		List<RelationMember> availableMembers = new ArrayList<RelationMember>(valueMembers);
		for (int i=oldValueMembers.size()-1; i>=0; i--) {
			if (findMember(availableMembers, oldValueMembers.get(i)) == null) {
				operationList.add(operationFactory.newOperation(value.getId(), VgiGeometryType.RELATION, VgiOperationType.OP_REMOVE_MEMBER, value.getUid(), value.getUsername(), value.getTimestamp(), (short)value.getVersion(), (int)value.getChangesetId(), null, "", "", 0, i));
				/** Remove node from current node list */
				currentMembers.remove(i);
			} else {
				/** Remove node from available node list */
				availableMembers.remove(findMember(availableMembers, oldValueMembers.get(i)));
			}
		}
		
		
		/** PHASE B: Find OpAddNode operations */
		availableMembers = new ArrayList<RelationMember>(oldValueMembers);
		for (int i=0; i<valueMembers.size(); i++) {
			if (findMember(availableMembers, valueMembers.get(i)) == null) {
				//TODO change memberType to EntityType
				String memberType = ((valueMembers.get(i).equals(OsmElementType.NODE)) ? "n" : (valueMembers.get(i).equals(OsmElementType.WAY)) ? "w" : (valueMembers.get(i).equals(OsmElementType.RELATION)) ? "r" : "");
				operationList.add(operationFactory.newOperation(value.getId(), VgiGeometryType.RELATION, VgiOperationType.OP_ADD_MEMBER, value.getUid(), value.getUsername(), value.getTimestamp(), (short)value.getVersion(), (int)value.getChangesetId(), null, memberType, valueMembers.get(i).getRole(), valueMembers.get(i).getId(), i));
				/** Adds node to current node list */
				currentMembers.add(i, valueMembers.get(i));
			} else {
				/** Remove node from available node list */
				availableMembers.remove(findMember(availableMembers, valueMembers.get(i)));
			}
		}
		
		if (currentMembers.size() != valueMembers.size()) {
			log.error("Current Node Ref List has " + currentMembers.size() + " items, while Value Node Ref List has " + valueMembers.size() + " items");
			log.info(" - Relation " + value.getId() + " v" + value.getVersion());
			String listString = "";
			for (RelationMember s : currentMembers) {
			    listString += s.getId() + "  ";
			}
			log.info(" - " + listString);
			listString = "";
			for (RelationMember s : valueMembers) {
			    listString += s.getId() + "  ";
			}
			log.info(" - " + listString);
		}
		
		/** PHASE C: Find OpReorderNode operations */
//		int position = 0;
//		while(position < currentMembers.size()) {
		for (int position = 0; position < currentMembers.size(); position++) {
//			if (!currentMembers.get(position).equals(valueMembers.get(position))) { //TODO equals cannot work
			if (getRelationMemberComparator().compare(currentMembers.get(position), valueMembers.get(position)) != 0) {
				/** Find next valueNodeRef in remaining currentNodeRefs */
				for (int i=position+1; i<currentMembers.size(); i++) {
//					if (currentMembers.get(i).equals(valueMembers.get(position))) { //TODO equals cannot work
					if (getRelationMemberComparator().compare(currentMembers.get(i), valueMembers.get(position)) != 0) {
						operationList.add(operationFactory.newOperation(value.getId(), VgiGeometryType.RELATION, VgiOperationType.OP_REORDER_MEMBER, value.getUid(), value.getUsername(), value.getTimestamp(), (short)value.getVersion(), (int)value.getChangesetId(), null, "", "", i, position));
						/** Reorder node within node list */
						currentMembers.remove(i);
						currentMembers.add(position, valueMembers.get(position));
						break;
					}
				}
			}
//			position++;
		}
		

		//TODO test
		/** PHASE D: Find OpModifyRole operations */
		for (int i=0; i<currentMembers.size(); i++) {
			if (!currentMembers.get(i).getRole().equals(valueMembers.get(i).getRole())) {
				operationList.add(operationFactory.newOperation(value.getId(), VgiGeometryType.RELATION, VgiOperationType.OP_MODIFY_ROLE, value.getUid(), value.getUsername(), value.getTimestamp(), (short)value.getVersion(), (int)value.getChangesetId(), null, "", valueMembers.get(i).getRole(), -1, i));
			}
		}
		
		return operationList;
	}
	
	/**
	 * Checks if IOsmEntry is contained in memberList
	 * @param memberList
	 * @param member
	 * @return
	 */
	private RelationMember findMember(List<RelationMember> memberList, RelationMember member) {
		for (RelationMember iMember : memberList) {
			if (getRelationMemberComparator().compare(iMember, member) == 0) {
				return iMember;
			}
		}
		return null;
	}
	
    public static Comparator<RelationMember> getRelationMemberComparator() {
        return new Comparator<RelationMember>() {
        	/**
        	 * Compares two <code>RelationMember</code> by member type and id
        	 * @param m1
        	 * @param m2
        	 * @return
        	 */
			public int compare(RelationMember m1, RelationMember m2) {
				if ((m1.getElementType().equals(OsmElementType.NODE) && m2.getElementType().equals(OsmElementType.NODE))
						|| (m1.getElementType().equals(OsmElementType.WAY) && m2.getElementType().equals(OsmElementType.WAY))
						|| (m1.getElementType().equals(OsmElementType.RELATION) && m2.getElementType().equals(OsmElementType.RELATION))) {
					if (m1.getId() == m2.getId()) {
						return 0;
					} else if (m1.getId() > m2.getId()) {
						return 1;
					} else {
						return -1;
					}
				} else if (m1.getElementType().equals(OsmElementType.NODE) && !(m2.getElementType().equals(OsmElementType.NODE))) {
					return -1;
				} else if (m1.getElementType().equals(OsmElementType.WAY) && !(m2.getElementType().equals(OsmElementType.WAY))) {
					return -1;
				} else {
					return 0;
				}
			}
        };
    }
}
