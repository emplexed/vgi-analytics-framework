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
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.vividsolutions.jts.geom.Coordinate;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Node;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.OsmElement;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.OsmElementType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Relation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Way;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiModelFactory;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.IOsmVgiNodeOperationGenerator;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.IOsmVgiOperationGenerator;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.IOsmVgiRelationOperationGenerator;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.IOsmVgiWayOperationGenerator;

public class VgiOperationGeneratorOsmImpl extends AbstractVgiOperationOsmGenerator implements IOsmVgiOperationGenerator {
	private static Logger log = Logger.getLogger(VgiOperationImpl.class);

	protected IOsmVgiNodeOperationGenerator nodeOpGenerator;
	protected IOsmVgiWayOperationGenerator wayOpGenerator;
	protected IOsmVgiRelationOperationGenerator relationOpGenerator;

	private VgiGeometryType geometryType = VgiGeometryType.UNDEFINED;
	
	public VgiOperationGeneratorOsmImpl(IVgiModelFactory operationFactory) {
		super(operationFactory);
		log.info("Initialze VGI Operation Generator");
	}
	
	/** Generate node related operations */
	@Override
	public List<IVgiOperation> generateNodeOperations(Node value, Node oldValue) {
		geometryType = VgiGeometryType.POINT;
		List<IVgiOperation> operations = nodeOpGenerator.generateNodeOperations(value, oldValue);
		operations.addAll(extractGeneralOperations(value, oldValue));
		return operations;
	}
	
	/** Generate way related operations */
	@Override
	public List<IVgiOperation> generateWayOperations(Way value, Way oldValue) {
		geometryType = VgiGeometryType.LINE;
		List<IVgiOperation> operations = wayOpGenerator.generateWayOperations(value, oldValue);
		operations.addAll(extractGeneralOperations(value, oldValue));
		return operations;
	}
	
	/** Generate relation related operations */
	@Override
	public List<IVgiOperation> generateRelationOperations(Relation value, Relation oldValue) {
		geometryType = VgiGeometryType.RELATION;
		List<IVgiOperation> operations = relationOpGenerator.generateRelationOperations(value, oldValue);
		operations.addAll(extractGeneralOperations(value, oldValue));
		return operations;
	}
	
	private List<IVgiOperation> extractGeneralOperations(OsmElement value, OsmElement oldValue) {
		List<IVgiOperation> operationList = this.compareTags(value, oldValue);
		operationList.addAll(this.checkIfDeleted(value, oldValue));
		return operationList;
	}
	
	/** Generate tag related operation */
	@Override
	public List<IVgiOperation> compareTags (OsmElement value, OsmElement oldValue) {
		List<IVgiOperation> operationList = new ArrayList<IVgiOperation>();

	    /** run through value's keys in order to find added and updated tags */
		Iterator<String> itr = value.getTags().keySet().iterator();
		while (itr.hasNext()) {
			String key = itr.next();
			
			if ((oldValue == null) || (!oldValue.getTags().containsKey(key))) {
				/** ADDED tag (key does not exist in oldValue) */
				operationList.add(operationFactory.newOperation(value.getId(), geometryType, VgiOperationType.OP_ADD_TAG, value.getUid(), value.getUsername(), value.getTimestamp(), (short)value.getVersion(), (int)value.getChangesetId(), null, key, value.getTags().get(key), -1, -1));
			} else {
				if (!value.getTags().get(key).equals(oldValue.getTags().get(key))) {
					/** UPDATED tag (key does exist in oldValue, but value is different) */
					operationList.add(operationFactory.newOperation(value.getId(), geometryType, VgiOperationType.OP_MODIFY_TAG_VALUE, value.getUid(), value.getUsername(), value.getTimestamp(), (short)value.getVersion(), (int)value.getChangesetId(), null, key, value.getTags().get(key), -1, -1));
				}
			}
		}
		
		if (oldValue != null) {
		    /** run through oldValue keys in order to find deleted tags */
		    itr = oldValue.getTags().keySet().iterator();
			while (itr.hasNext()) {
				String key = itr.next();
				if (!value.getTags().containsKey(key)) {
					/** REMOVED tag (key does not exist in value) */
					operationList.add(operationFactory.newOperation(value.getId(), geometryType, VgiOperationType.OP_REMOVE_TAG, value.getUid(), value.getUsername(), value.getTimestamp(), (short)value.getVersion(), (int)value.getChangesetId(), null, key, "", -1, -1));
				}
			}
		}
		
		return operationList;
	}
	
	/** Check if feature has been deleted or re-created<br />
	 *  Delete operation:<br />
	 *  	- oldValue is visible & value is invisible => OpDelete<br /> 
	 *  	- oldValue is null & value is invisible => OpDelete<br />
	 *  	- oldValue is invisible & value is invisible => No Operation<br />
	 *  Recreate operation<br />
	 *  	- oldValue is invisible & value is visible => OpRecreate
	 *  */
	@Override
	public List<IVgiOperation> checkIfDeleted(OsmElement value, OsmElement oldValue) {
		List<IVgiOperation> operationList = new ArrayList<IVgiOperation>();
		
		if (!value.isVisible() && (oldValue == null || oldValue.isVisible())) {
			if (value.getElementType().equals(OsmElementType.NODE)) {
				operationList.add(operationFactory.newOperation(value.getId(), geometryType,
						VgiOperationType.OP_DELETE_NODE, value.getUid(), value.getUsername(), value.getTimestamp(),
						(short) value.getVersion(), (int) value.getChangesetId(), null, "", "", -1, -1));
			} else if (value.getElementType().equals(OsmElementType.WAY)) {
				operationList.add(operationFactory.newOperation(value.getId(), geometryType,
						VgiOperationType.OP_DELETE_WAY, value.getUid(), value.getUsername(), value.getTimestamp(),
						(short) value.getVersion(), (int) value.getChangesetId(), null, "", "", -1, -1));
			} else if (value.getElementType().equals(OsmElementType.RELATION)) {
				operationList.add(operationFactory.newOperation(value.getId(), geometryType,
						VgiOperationType.OP_DELETE_RELATION, value.getUid(), value.getUsername(), value.getTimestamp(),
						(short) value.getVersion(), (int) value.getChangesetId(), null, "", "", -1, -1));
			}
		}
		
		if (value.isVisible() && (oldValue != null && !oldValue.isVisible())) {
			if (value.getElementType().equals(OsmElementType.NODE)) {
				Coordinate coordinate = new Coordinate(((Node) value).getCoordinate().x,
						((Node) value).getCoordinate().y);
				operationList.add(operationFactory.newOperation(value.getId(), geometryType,
						VgiOperationType.OP_RECREATE_NODE, value.getUid(), value.getUsername(), value.getTimestamp(),
						(short) value.getVersion(), (int) value.getChangesetId(), coordinate, "", "", -1, -1));
			} else if (value.getElementType().equals(OsmElementType.WAY)) {
				operationList.add(operationFactory.newOperation(value.getId(), geometryType,
						VgiOperationType.OP_RECREATE_WAY, value.getUid(), value.getUsername(), value.getTimestamp(),
						(short) value.getVersion(), (int) value.getChangesetId(), null, "", "", -1, -1));
			} else if (value.getElementType().equals(OsmElementType.RELATION)) {
				operationList.add(operationFactory.newOperation(value.getId(), geometryType,
						VgiOperationType.OP_RECREATE_RELATION, value.getUid(), value.getUsername(),
						value.getTimestamp(), (short) value.getVersion(), (int) value.getChangesetId(), null, "", "",
						-1, -1));
			}
		}
		
		return operationList;
	}
	
	@Override
	public IOsmVgiNodeOperationGenerator getNodeOpGenerator() {
		return nodeOpGenerator;
	}
	@Override
	public void setNodeOpGenerator(IOsmVgiNodeOperationGenerator nodeOpGenerator) {
		this.nodeOpGenerator = nodeOpGenerator;
	}

	@Override
	public IOsmVgiWayOperationGenerator getWayOpGenerator() {
		return wayOpGenerator;
	}
	@Override
	public void setWayOpGenerator(IOsmVgiWayOperationGenerator wayOpGenerator) {
		this.wayOpGenerator = wayOpGenerator;
	}

	@Override
	public IOsmVgiRelationOperationGenerator getRelationOpGenerator() {
		return relationOpGenerator;
	}
	@Override
	public void setRelationOpGenerator(IOsmVgiRelationOperationGenerator relationOpGenerator) {
		this.relationOpGenerator = relationOpGenerator;
	}
}
