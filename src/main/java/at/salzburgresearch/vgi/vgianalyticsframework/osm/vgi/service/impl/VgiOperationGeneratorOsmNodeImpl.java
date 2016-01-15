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
import java.util.List;

import org.apache.log4j.Logger;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.io.WKBWriter;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Node;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiModelFactory;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.IOsmVgiNodeOperationGenerator;

public class VgiOperationGeneratorOsmNodeImpl extends AbstractVgiOperationOsmGenerator implements IOsmVgiNodeOperationGenerator {
	private static Logger log = Logger.getLogger(VgiOperationGeneratorOsmNodeImpl.class);
	
	WKBWriter wkbWriter = new WKBWriter(2);
	
	public VgiOperationGeneratorOsmNodeImpl(IVgiModelFactory operationFactory) {
		super(operationFactory);
		log.info("Initialze VGI Node Operation Generator"); 
	}
	
	/**
	 * Extracts VGI Operations from geographic data model. in this implementation OSM model is used as input
	 * 
	 * if no operations are found an empty list is returned (null save)
	 * 
	 * 
	 * @param value: current version of the node
	 * @param oldVaue: previous version of the node 
	 */
	@Override
	public List<IVgiOperation> generateNodeOperations(Node value, Node oldValue) {
		/**
		 *  (!) Since the OSM history planet file from 2013-02-08, invisible nodes do not include a coordinate
		 */
		List<IVgiOperation> ops = new ArrayList<IVgiOperation>();
		if (oldValue != null) {
			if (value.getCoordinate() == null || oldValue.getCoordinate() == null) return ops;
			if ((value.getCoordinate().x != oldValue.getCoordinate().x) || (value.getCoordinate().y != oldValue.getCoordinate().y)) {
				ops.add(operationFactory.newOperation(value.getId(), VgiGeometryType.POINT, VgiOperationType.OP_MODIFY_COORDINATE, value.getUid(), value.getUsername(), value.getTimestamp(), (short)value.getVersion(), (int)value.getChangesetId(), new Coordinate(value.getCoordinate().x, value.getCoordinate().y), "", "", -1, -1));
			}
		} else {
			/** Version 1 */
			if (value.getCoordinate() != null) {
				ops.add(operationFactory.newOperation(value.getId(), VgiGeometryType.POINT, VgiOperationType.OP_CREATE_NODE, value.getUid(), value.getUsername(), value.getTimestamp(), (short)value.getVersion(), (int)value.getChangesetId(), new Coordinate(value.getCoordinate().x, value.getCoordinate().y), "", "", -1, -1));
			} else {
				ops.add(operationFactory.newOperation(value.getId(), VgiGeometryType.POINT, VgiOperationType.OP_CREATE_NODE, value.getUid(), value.getUsername(), value.getTimestamp(), (short)value.getVersion(), (int)value.getChangesetId(), null, "","", -1, -1));
			}
		}
		
		return ops;
	}
}
