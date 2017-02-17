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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Way;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiModelFactory;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiModelFactoryImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.impl.VgiOperationGeneratorOsmWayImpl;
import junit.framework.Assert;

public class TestVgiOperationGeneratorOsmWay {
	
	@Test
	public void testCreateWay() {
		Way w2 = new Way(1);
		List<Long> nodelist2 = new ArrayList<Long>();
		nodelist2.add(1l);
		nodelist2.add(2l);
		nodelist2.add(3l);
		nodelist2.add(4l);
		w2.setWayNodes(nodelist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmWayImpl opGenerator = new VgiOperationGeneratorOsmWayImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateWayOperations(w2, null);
		
		Assert.assertEquals(5, operations.size());

		Assert.assertEquals(VgiOperationType.OP_CREATE_WAY, operations.get(0).getVgiOperationType());
		Assert.assertEquals(VgiOperationType.OP_ADD_NODE, operations.get(1).getVgiOperationType());
		Assert.assertEquals(1, operations.get(1).getRefId());
	}
	
	@Test
    public void testAddNode() {
		Way w1 = new Way(1);
		List<Long> nodelist1 = new ArrayList<Long>();
		nodelist1.add(1l);
		nodelist1.add(2l);
		nodelist1.add(3l);
		w1.setWayNodes(nodelist1);
		
		Way w2 = new Way(1);
		List<Long> nodelist2 = new ArrayList<Long>();
		nodelist2.add(1l);
		nodelist2.add(2l);
		nodelist2.add(3l);
		nodelist2.add(4l);
		w2.setWayNodes(nodelist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmWayImpl opGenerator = new VgiOperationGeneratorOsmWayImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateWayOperations(w2, w1);
		
		Assert.assertEquals(1, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_ADD_NODE, operations.get(0).getVgiOperationType());
		Assert.assertEquals(4, operations.get(0).getRefId());
	}
	
	@Test
    public void testRemoveNode() {
		Way w1 = new Way(1);
		List<Long> nodelist1 = new ArrayList<Long>();
		nodelist1.add(1l);
		nodelist1.add(2l);
		nodelist1.add(3l);
		nodelist1.add(4l);
		w1.setWayNodes(nodelist1);
		
		Way w2 = new Way(1);
		List<Long> nodelist2 = new ArrayList<Long>();
		nodelist2.add(1l);
		nodelist2.add(2l);
		nodelist2.add(3l);
		w2.setWayNodes(nodelist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmWayImpl opGenerator = new VgiOperationGeneratorOsmWayImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateWayOperations(w2, w1);
		
		Assert.assertEquals(1, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_REMOVE_NODE, operations.get(0).getVgiOperationType());
		Assert.assertEquals(3, operations.get(0).getPosition());
	}
	
	@Test
    public void testAddAndRemoveNodes() {
		Way w1 = new Way(1);
		List<Long> nodelist1 = new ArrayList<Long>();
		nodelist1.add(1l);
		nodelist1.add(2l);
		nodelist1.add(3l);
		nodelist1.add(4l);
		w1.setWayNodes(nodelist1);
		
		Way w2 = new Way(1);
		List<Long> nodelist2 = new ArrayList<Long>();
		nodelist2.add(1l);
		nodelist2.add(5l);
		nodelist2.add(6l);
		nodelist2.add(3l);
		w2.setWayNodes(nodelist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmWayImpl opGenerator = new VgiOperationGeneratorOsmWayImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateWayOperations(w2, w1);
		
		Assert.assertEquals(4, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_REMOVE_NODE, operations.get(0).getVgiOperationType());
		Assert.assertEquals(3, operations.get(0).getPosition());
		Assert.assertEquals(VgiOperationType.OP_REMOVE_NODE, operations.get(1).getVgiOperationType());
		Assert.assertEquals(1, operations.get(1).getPosition());
		Assert.assertEquals(VgiOperationType.OP_ADD_NODE, operations.get(2).getVgiOperationType());
		Assert.assertEquals(5, operations.get(2).getRefId());
		Assert.assertEquals(1, operations.get(2).getPosition());
		Assert.assertEquals(VgiOperationType.OP_ADD_NODE, operations.get(3).getVgiOperationType());
		Assert.assertEquals(6, operations.get(3).getRefId());
		Assert.assertEquals(2, operations.get(3).getPosition());
	}
	
	@Test
    public void testReorderNode() {
		Way w1 = new Way(1);
		List<Long> nodelist1 = new ArrayList<Long>();
		nodelist1.add(1l);
		nodelist1.add(2l);
		nodelist1.add(3l);
		w1.setWayNodes(nodelist1);
		
		Way w2 = new Way(1);
		List<Long> nodelist2 = new ArrayList<Long>();
		nodelist2.add(1l);
		nodelist2.add(3l);
		nodelist2.add(2l);
		w2.setWayNodes(nodelist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmWayImpl opGenerator = new VgiOperationGeneratorOsmWayImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateWayOperations(w2, w1);
		
		Assert.assertEquals(1, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_REORDER_NODE, operations.get(0).getVgiOperationType());
		Assert.assertEquals(1, operations.get(0).getPosition());
		Assert.assertEquals(2, operations.get(0).getRefId());
	}
}
