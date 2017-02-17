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

package at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service;

import java.util.List;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Node;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiModelFactory;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiModelFactoryImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.impl.VgiOperationGeneratorOsmNodeImpl;
import junit.framework.Assert;

public class TestVgiOperationGeneratorOsmNode {
	
	@Test
	public void testCreateNode() {
		Node n2 = new Node(1);
		n2.setCoordinate(new Coordinate(13, 46));
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmNodeImpl opGenerator = new VgiOperationGeneratorOsmNodeImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateNodeOperations(n2, null);
		
		Assert.assertEquals(1, operations.size());

		Assert.assertEquals(VgiOperationType.OP_CREATE_NODE, operations.get(0).getVgiOperationType());
		Assert.assertEquals(1, operations.get(0).getOid());
	}
	
	@Test
    public void testUpdateCoordinate() {
		Node n1 = new Node(1);
		n1.setCoordinate(new Coordinate(13.0, 46.0));
		
		Node n2 = new Node(1);
		n2.setCoordinate(new Coordinate(13.5, 47.0));
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmNodeImpl opGenerator = new VgiOperationGeneratorOsmNodeImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateNodeOperations(n2, n1);
		
		Assert.assertEquals(1, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_MODIFY_COORDINATE, operations.get(0).getVgiOperationType());
		Assert.assertEquals(13.5, operations.get(0).getCoordinate().x);
		Assert.assertEquals(47.0, operations.get(0).getCoordinate().y);
	}
}
