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

import org.junit.Assert;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Node;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.OsmElementType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Relation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.RelationMember;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Way;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiModelFactory;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiModelFactoryImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.impl.VgiOperationGeneratorOsmImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.impl.VgiOperationGeneratorOsmNodeImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.impl.VgiOperationGeneratorOsmRelationImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.impl.VgiOperationGeneratorOsmWayImpl;

public class TestVgiOperationGeneratorOsm {
	
	@Test
    public void testNodeOpGenerator() {
		Node n1 = new Node(1);
		n1.setCoordinate(new Coordinate(13.0, 46.0));
		
		Node n2 = new Node(1);
		n2.setCoordinate(new Coordinate(13.5, 47.0));
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmImpl opGenerator = new VgiOperationGeneratorOsmImpl(factory);
		VgiOperationGeneratorOsmNodeImpl nodeOpGenerator = new VgiOperationGeneratorOsmNodeImpl(factory);
		opGenerator.setNodeOpGenerator(nodeOpGenerator);
		
		List<IVgiOperation> operations = opGenerator.generateNodeOperations(n2, n1);
		
		Assert.assertEquals(1, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_MODIFY_COORDINATE, operations.get(0).getVgiOperationType());
		Assert.assertEquals(13.5, operations.get(0).getCoordinate().x, 0.0);
		Assert.assertEquals(47.0, operations.get(0).getCoordinate().y, 0.0);
	}
	
	@Test
    public void testAddTag() {
		Node n1 = new Node(1);
		n1.setCoordinate(new Coordinate(13.0, 46.0));
		
		Node n2 = new Node(1);
		n2.setCoordinate(new Coordinate(13.0, 46.0));
		n2.getTags().put("highway", "motorway");
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmImpl opGenerator = new VgiOperationGeneratorOsmImpl(factory);
		VgiOperationGeneratorOsmNodeImpl nodeOpGenerator = new VgiOperationGeneratorOsmNodeImpl(factory);
		opGenerator.setNodeOpGenerator(nodeOpGenerator);
		
		List<IVgiOperation> operations = opGenerator.generateNodeOperations(n2, n1);
		
		Assert.assertEquals(1, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_ADD_TAG, operations.get(0).getVgiOperationType());
		Assert.assertEquals("highway", operations.get(0).getKey());
		Assert.assertEquals("motorway", operations.get(0).getValue());
	}
	
	@Test
    public void testRemoveTag() {
		Node n1 = new Node(1);
		n1.setCoordinate(new Coordinate(13.0, 46.0));
		n1.getTags().put("highway", "motorway");
		n1.getTags().put("maxspeed", "50");
		n1.getTags().put("oneway", "yes");
		
		Node n2 = new Node(1);
		n2.setCoordinate(new Coordinate(13.0, 46.0));
		n2.getTags().put("highway", "motorway");
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmImpl opGenerator = new VgiOperationGeneratorOsmImpl(factory);
		VgiOperationGeneratorOsmNodeImpl nodeOpGenerator = new VgiOperationGeneratorOsmNodeImpl(factory);
		opGenerator.setNodeOpGenerator(nodeOpGenerator);
		
		List<IVgiOperation> operations = opGenerator.generateNodeOperations(n2, n1);
		
		Assert.assertEquals(2, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_REMOVE_TAG, operations.get(0).getVgiOperationType());
		Assert.assertEquals("maxspeed", operations.get(0).getKey());
		Assert.assertEquals("", operations.get(0).getValue());
		Assert.assertEquals(VgiOperationType.OP_REMOVE_TAG, operations.get(1).getVgiOperationType());
		Assert.assertEquals("oneway", operations.get(1).getKey());
	}
	
	@Test
    public void testModifyTag() {
		Node n1 = new Node(1);
		n1.setCoordinate(new Coordinate(13.0, 46.0));
		n1.getTags().put("highway", "motorway");
		n1.getTags().put("maxspeed", "50");
		n1.getTags().put("oneway", "yes");
		
		Node n2 = new Node(1);
		n2.setCoordinate(new Coordinate(13.0, 46.0));
		n2.getTags().put("highway", "motorway");
		n2.getTags().put("maxspeed", "60");
		n2.getTags().put("oneway", "no");
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmImpl opGenerator = new VgiOperationGeneratorOsmImpl(factory);
		VgiOperationGeneratorOsmNodeImpl nodeOpGenerator = new VgiOperationGeneratorOsmNodeImpl(factory);
		opGenerator.setNodeOpGenerator(nodeOpGenerator);
		
		List<IVgiOperation> operations = opGenerator.generateNodeOperations(n2, n1);
		
		Assert.assertEquals(2, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_MODIFY_TAG_VALUE, operations.get(0).getVgiOperationType());
		Assert.assertEquals("maxspeed", operations.get(0).getKey());
		Assert.assertEquals("60", operations.get(0).getValue());
		Assert.assertEquals(VgiOperationType.OP_MODIFY_TAG_VALUE, operations.get(1).getVgiOperationType());
		Assert.assertEquals("oneway", operations.get(1).getKey());
		Assert.assertEquals("no", operations.get(1).getValue());
	}
	
	@Test
	public void testDeleteNode() {
		Node n1 = new Node(1);
		n1.setCoordinate(new Coordinate(13.0, 46.0));
		n1.setVisible(true);

		Node n2 = new Node(1);
		n2.setVisible(false);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmImpl opGenerator = new VgiOperationGeneratorOsmImpl(factory);
		VgiOperationGeneratorOsmNodeImpl nodeOpGenerator = new VgiOperationGeneratorOsmNodeImpl(factory);
		opGenerator.setNodeOpGenerator(nodeOpGenerator);
		
		List<IVgiOperation> operations = opGenerator.generateNodeOperations(n2, n1);
		
		Assert.assertEquals(1, operations.size());

		Assert.assertEquals(VgiOperationType.OP_DELETE_NODE, operations.get(0).getVgiOperationType());
		Assert.assertEquals(1l, operations.get(0).getOid());
	}
	
	@Test
	public void testRecreateNode() {
		Node n1 = new Node(1);
		n1.setVisible(false);
		
		Node n2 = new Node(1);
		n2.setCoordinate(new Coordinate(13.0, 46.0));
		n2.setVisible(true);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmImpl opGenerator = new VgiOperationGeneratorOsmImpl(factory);
		VgiOperationGeneratorOsmNodeImpl nodeOpGenerator = new VgiOperationGeneratorOsmNodeImpl(factory);
		opGenerator.setNodeOpGenerator(nodeOpGenerator);
		
		List<IVgiOperation> operations = opGenerator.generateNodeOperations(n2, n1);
		
		Assert.assertEquals(1, operations.size());

		Assert.assertEquals(VgiOperationType.OP_RECREATE_NODE, operations.get(0).getVgiOperationType());
		Assert.assertEquals(1l, operations.get(0).getOid());
	}
	
	@Test
	public void testDeleteWay() {
		Way w1 = new Way(2);
		List<Long> nodelist1 = new ArrayList<Long>();
		nodelist1.add(1l);
		nodelist1.add(2l);
		w1.setWayNodes(nodelist1);
		w1.setVisible(true);
		
		Way w2 = new Way(2);
		w2.setWayNodes(new ArrayList<Long>());
		w2.setVisible(false);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmImpl opGenerator = new VgiOperationGeneratorOsmImpl(factory);
		VgiOperationGeneratorOsmWayImpl wayOpGenerator = new VgiOperationGeneratorOsmWayImpl(factory);
		opGenerator.setWayOpGenerator(wayOpGenerator);
		
		List<IVgiOperation> operations = opGenerator.generateWayOperations(w2, w1);
		
		Assert.assertEquals(3, operations.size());

		Assert.assertEquals(VgiOperationType.OP_REMOVE_NODE, operations.get(0).getVgiOperationType());
		Assert.assertEquals(1, operations.get(0).getPosition());
		Assert.assertEquals(VgiOperationType.OP_REMOVE_NODE, operations.get(1).getVgiOperationType());
		Assert.assertEquals(0, operations.get(1).getPosition());
		Assert.assertEquals(VgiOperationType.OP_DELETE_WAY, operations.get(2).getVgiOperationType());
		Assert.assertEquals(2, operations.get(2).getOid());
	}
	
	@Test
	public void testDeleteRelation() {
		Relation r1 = new Relation(3);
		List<RelationMember> memberlist1 = new ArrayList<RelationMember>();
		memberlist1.add(new RelationMember(1, OsmElementType.NODE, ""));
		r1.setMembers(memberlist1);
		r1.setVisible(true);
		
		Relation r2 = new Relation(3);
		List<RelationMember> memberlist2 = new ArrayList<RelationMember>();
		r2.setMembers(memberlist2);
		r2.setVisible(false);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmImpl opGenerator = new VgiOperationGeneratorOsmImpl(factory);
		VgiOperationGeneratorOsmRelationImpl relationOpGenerator = new VgiOperationGeneratorOsmRelationImpl(factory);
		opGenerator.setRelationOpGenerator(relationOpGenerator);
		
		List<IVgiOperation> operations = opGenerator.generateRelationOperations(r2, r1);
		
		Assert.assertEquals(2, operations.size());

		Assert.assertEquals(VgiOperationType.OP_REMOVE_MEMBER, operations.get(0).getVgiOperationType());
		Assert.assertEquals(0, operations.get(0).getPosition());
		Assert.assertEquals(VgiOperationType.OP_DELETE_RELATION, operations.get(1).getVgiOperationType());
		Assert.assertEquals(3, operations.get(1).getOid());
	}
	
	
}
