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
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.OsmElementType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Relation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.RelationMember;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiModelFactory;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiModelFactoryImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.impl.VgiOperationGeneratorOsmRelationImpl;
import junit.framework.Assert;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/application-context-osm-op-generator.xml"})
public class TestVgiOperationGeneratorOsmRelation {
	
	@Test
	public void testCreateRelation() {
		Relation r2 = new Relation(1);
		List<RelationMember> memberlist2 = new ArrayList<RelationMember>();
		memberlist2.add(new RelationMember(1, OsmElementType.NODE, ""));
		memberlist2.add(new RelationMember(2, OsmElementType.NODE, ""));
		memberlist2.add(new RelationMember(3, OsmElementType.NODE, ""));
		memberlist2.add(new RelationMember(4, OsmElementType.NODE, ""));
		r2.setMembers(memberlist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmRelationImpl opGenerator = new VgiOperationGeneratorOsmRelationImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateRelationOperations(r2, null);
		
		Assert.assertEquals(5, operations.size());

		Assert.assertEquals(VgiOperationType.OP_CREATE_RELATION, operations.get(0).getVgiOperationType());
		Assert.assertEquals(VgiOperationType.OP_ADD_MEMBER, operations.get(1).getVgiOperationType());
		Assert.assertEquals(1, operations.get(1).getRefId());
	}
	
	@Test
    public void testAddNodeMember() {
		Relation r1 = new Relation(1);
		List<RelationMember> memberlist1 = new ArrayList<RelationMember>();
		memberlist1.add(new RelationMember(1, OsmElementType.NODE, ""));
		memberlist1.add(new RelationMember(2, OsmElementType.NODE, ""));
		memberlist1.add(new RelationMember(3, OsmElementType.NODE, ""));
		r1.setMembers(memberlist1);
		
		Relation r2 = new Relation(1);
		List<RelationMember> memberlist2 = new ArrayList<RelationMember>();
		memberlist2.add(new RelationMember(1, OsmElementType.NODE, ""));
		memberlist2.add(new RelationMember(2, OsmElementType.NODE, ""));
		memberlist2.add(new RelationMember(3, OsmElementType.NODE, ""));
		memberlist2.add(new RelationMember(4, OsmElementType.NODE, ""));
		r2.setMembers(memberlist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmRelationImpl opGenerator = new VgiOperationGeneratorOsmRelationImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateRelationOperations(r2, r1);
		
		Assert.assertEquals(1, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_ADD_MEMBER, operations.get(0).getVgiOperationType());
		Assert.assertEquals("n", operations.get(0).getKey());
	}
	
	@Test
    public void testAddWayMember() {
		Relation r1 = new Relation(1);
		List<RelationMember> memberlist1 = new ArrayList<RelationMember>();
		memberlist1.add(new RelationMember(1, OsmElementType.WAY, ""));
		memberlist1.add(new RelationMember(2, OsmElementType.WAY, ""));
		memberlist1.add(new RelationMember(3, OsmElementType.WAY, ""));
		r1.setMembers(memberlist1);
		
		Relation r2 = new Relation(1);
		List<RelationMember> memberlist2 = new ArrayList<RelationMember>();
		memberlist2.add(new RelationMember(1, OsmElementType.WAY, ""));
		memberlist2.add(new RelationMember(2, OsmElementType.WAY, ""));
		memberlist2.add(new RelationMember(3, OsmElementType.WAY, ""));
		memberlist2.add(new RelationMember(4, OsmElementType.WAY, ""));
		r2.setMembers(memberlist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmRelationImpl opGenerator = new VgiOperationGeneratorOsmRelationImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateRelationOperations(r2, r1);
		
		Assert.assertEquals(1, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_ADD_MEMBER, operations.get(0).getVgiOperationType());
		Assert.assertEquals("w", operations.get(0).getKey());
	}
	
	@Test
    public void testAddRelationMember() {
		Relation r1 = new Relation(1);
		List<RelationMember> memberlist1 = new ArrayList<RelationMember>();
		memberlist1.add(new RelationMember(1, OsmElementType.RELATION, ""));
		memberlist1.add(new RelationMember(2, OsmElementType.RELATION, ""));
		memberlist1.add(new RelationMember(3, OsmElementType.RELATION, ""));
		r1.setMembers(memberlist1);
		
		Relation r2 = new Relation(1);
		List<RelationMember> memberlist2 = new ArrayList<RelationMember>();
		memberlist2.add(new RelationMember(1, OsmElementType.RELATION, ""));
		memberlist2.add(new RelationMember(2, OsmElementType.RELATION, ""));
		memberlist2.add(new RelationMember(3, OsmElementType.RELATION, ""));
		memberlist2.add(new RelationMember(4, OsmElementType.RELATION, ""));
		r2.setMembers(memberlist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmRelationImpl opGenerator = new VgiOperationGeneratorOsmRelationImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateRelationOperations(r2, r1);
		
		Assert.assertEquals(1, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_ADD_MEMBER, operations.get(0).getVgiOperationType());
		Assert.assertEquals("r", operations.get(0).getKey());
	}
	
	@Test
    public void testRemoveNodeMember() {
		Relation r1 = new Relation(1);
		List<RelationMember> memberlist1 = new ArrayList<RelationMember>();
		memberlist1.add(new RelationMember(1, OsmElementType.NODE, ""));
		memberlist1.add(new RelationMember(2, OsmElementType.NODE, ""));
		memberlist1.add(new RelationMember(3, OsmElementType.NODE, ""));
		memberlist1.add(new RelationMember(4, OsmElementType.NODE, ""));
		r1.setMembers(memberlist1);
		
		Relation r2 = new Relation(1);
		List<RelationMember> memberlist2 = new ArrayList<RelationMember>();
		memberlist2.add(new RelationMember(1, OsmElementType.NODE, ""));
		memberlist2.add(new RelationMember(2, OsmElementType.NODE, ""));
		memberlist2.add(new RelationMember(3, OsmElementType.NODE, ""));
		r2.setMembers(memberlist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmRelationImpl opGenerator = new VgiOperationGeneratorOsmRelationImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateRelationOperations(r2, r1);
		
		Assert.assertEquals(1, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_REMOVE_MEMBER, operations.get(0).getVgiOperationType());
	}
	
	@Test
    public void testRemoveWayMember() {
		Relation r1 = new Relation(1);
		List<RelationMember> memberlist1 = new ArrayList<RelationMember>();
		memberlist1.add(new RelationMember(1, OsmElementType.NODE, ""));
		memberlist1.add(new RelationMember(2, OsmElementType.WAY, ""));
		memberlist1.add(new RelationMember(3, OsmElementType.WAY, ""));
		memberlist1.add(new RelationMember(4, OsmElementType.WAY, ""));
		r1.setMembers(memberlist1);
		
		Relation r2 = new Relation(1);
		List<RelationMember> memberlist2 = new ArrayList<RelationMember>();
		memberlist2.add(new RelationMember(1, OsmElementType.NODE, ""));
		memberlist2.add(new RelationMember(2, OsmElementType.WAY, ""));
		memberlist2.add(new RelationMember(3, OsmElementType.WAY, ""));
		r2.setMembers(memberlist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmRelationImpl opGenerator = new VgiOperationGeneratorOsmRelationImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateRelationOperations(r2, r1);
		
		Assert.assertEquals(1, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_REMOVE_MEMBER, operations.get(0).getVgiOperationType());
		Assert.assertEquals(3, operations.get(0).getPosition());
	}
	
	@Test
    public void testRemoveRelationMember() {
		Relation r1 = new Relation(1);
		List<RelationMember> memberlist1 = new ArrayList<RelationMember>();
		memberlist1.add(new RelationMember(1, OsmElementType.WAY, ""));
		memberlist1.add(new RelationMember(2, OsmElementType.WAY, ""));
		memberlist1.add(new RelationMember(3, OsmElementType.RELATION, ""));
		memberlist1.add(new RelationMember(4, OsmElementType.RELATION, ""));
		r1.setMembers(memberlist1);
		
		Relation r2 = new Relation(1);
		List<RelationMember> memberlist2 = new ArrayList<RelationMember>();
		memberlist2.add(new RelationMember(1, OsmElementType.WAY, ""));
		memberlist2.add(new RelationMember(2, OsmElementType.WAY, ""));
		memberlist2.add(new RelationMember(3, OsmElementType.RELATION, ""));
		r2.setMembers(memberlist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmRelationImpl opGenerator = new VgiOperationGeneratorOsmRelationImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateRelationOperations(r2, r1);
		
		Assert.assertEquals(1, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_REMOVE_MEMBER, operations.get(0).getVgiOperationType());
	}
	
	@Test
    public void testAddAndRemoveMember() {
		Relation r1 = new Relation(1);
		List<RelationMember> memberlist1 = new ArrayList<RelationMember>();
		memberlist1.add(new RelationMember(1, OsmElementType.WAY, ""));
		memberlist1.add(new RelationMember(2, OsmElementType.WAY, ""));
		memberlist1.add(new RelationMember(3, OsmElementType.RELATION, ""));
		memberlist1.add(new RelationMember(4, OsmElementType.RELATION, ""));
		r1.setMembers(memberlist1);
		
		Relation r2 = new Relation(1);
		List<RelationMember> memberlist2 = new ArrayList<RelationMember>();
		memberlist2.add(new RelationMember(1, OsmElementType.WAY, ""));
		memberlist2.add(new RelationMember(5, OsmElementType.WAY, ""));
		memberlist2.add(new RelationMember(6, OsmElementType.RELATION, ""));
		memberlist2.add(new RelationMember(3, OsmElementType.RELATION, ""));
		r2.setMembers(memberlist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmRelationImpl opGenerator = new VgiOperationGeneratorOsmRelationImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateRelationOperations(r2, r1);
		
		Assert.assertEquals(4, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_REMOVE_MEMBER, operations.get(0).getVgiOperationType());
		Assert.assertEquals(3, operations.get(0).getPosition());
		Assert.assertEquals(VgiOperationType.OP_REMOVE_MEMBER, operations.get(1).getVgiOperationType());
		Assert.assertEquals(1, operations.get(1).getPosition());
		Assert.assertEquals(VgiOperationType.OP_ADD_MEMBER, operations.get(2).getVgiOperationType());
		Assert.assertEquals(5, operations.get(2).getRefId());
		Assert.assertEquals("w", operations.get(2).getKey());
		Assert.assertEquals(1, operations.get(2).getPosition());
		Assert.assertEquals(VgiOperationType.OP_ADD_MEMBER, operations.get(3).getVgiOperationType());
		Assert.assertEquals(6, operations.get(3).getRefId());
		Assert.assertEquals("r", operations.get(3).getKey());
		Assert.assertEquals(2, operations.get(3).getPosition());
	}
	
	@Test
    public void testRelation879() {
		Relation r1 = new Relation(879);
		List<RelationMember> memberlist1 = new ArrayList<RelationMember>();
		memberlist1.add(new RelationMember(315642822, OsmElementType.NODE, ""));
		memberlist1.add(new RelationMember(4870766, OsmElementType.WAY, "outer"));
		memberlist1.add(new RelationMember(8154192, OsmElementType.WAY, "inner"));
		memberlist1.add(new RelationMember(25692566, OsmElementType.WAY, ""));
		r1.setMembers(memberlist1);
		
		Relation r2 = new Relation(879);
		List<RelationMember> memberlist2 = new ArrayList<RelationMember>();
		memberlist2.add(new RelationMember(315642822, OsmElementType.NODE, ""));
		memberlist2.add(new RelationMember(315642830, OsmElementType.NODE, ""));
		memberlist2.add(new RelationMember(4870766, OsmElementType.WAY, "outer"));
		memberlist2.add(new RelationMember(8154192, OsmElementType.WAY, "inner"));
		memberlist2.add(new RelationMember(25692566, OsmElementType.WAY, ""));
		r2.setMembers(memberlist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmRelationImpl opGenerator = new VgiOperationGeneratorOsmRelationImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateRelationOperations(r2, r1);
		
		Assert.assertEquals(1, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_ADD_MEMBER, operations.get(0).getVgiOperationType());
		Assert.assertEquals(315642830, operations.get(0).getRefId());
		Assert.assertEquals("n", operations.get(0).getKey());
		Assert.assertEquals(1, operations.get(0).getPosition());
	}
	
	@Test
    public void testModifyRole() {
		Relation r1 = new Relation(1);
		List<RelationMember> memberlist1 = new ArrayList<RelationMember>();
		memberlist1.add(new RelationMember(1, OsmElementType.NODE, ""));
		memberlist1.add(new RelationMember(2, OsmElementType.NODE, ""));
		memberlist1.add(new RelationMember(3, OsmElementType.NODE, ""));
		r1.setMembers(memberlist1);
		
		Relation r2 = new Relation(1);
		List<RelationMember> memberlist2 = new ArrayList<RelationMember>();
		memberlist2.add(new RelationMember(1, OsmElementType.NODE, ""));
		memberlist2.add(new RelationMember(2, OsmElementType.NODE, "newrole"));
		memberlist2.add(new RelationMember(3, OsmElementType.NODE, ""));
		r2.setMembers(memberlist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmRelationImpl opGenerator = new VgiOperationGeneratorOsmRelationImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateRelationOperations(r2, r1);
		
		Assert.assertEquals(1, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_MODIFY_ROLE, operations.get(0).getVgiOperationType());
		Assert.assertEquals(1, operations.get(0).getPosition());
		Assert.assertEquals("newrole", operations.get(0).getValue());
	}
	
	@Test
    public void testReorderMember() {
		Relation r1 = new Relation(1);
		List<RelationMember> memberlist1 = new ArrayList<RelationMember>();
		memberlist1.add(new RelationMember(1, OsmElementType.NODE, ""));
		memberlist1.add(new RelationMember(2, OsmElementType.NODE, ""));
		memberlist1.add(new RelationMember(3, OsmElementType.NODE, ""));
		r1.setMembers(memberlist1);
		
		Relation r2 = new Relation(1);
		List<RelationMember> memberlist2 = new ArrayList<RelationMember>();
		memberlist2.add(new RelationMember(1, OsmElementType.NODE, ""));
		memberlist2.add(new RelationMember(3, OsmElementType.NODE, ""));
		memberlist2.add(new RelationMember(2, OsmElementType.NODE, ""));
		r2.setMembers(memberlist2);
		
		IVgiModelFactory factory = new VgiModelFactoryImpl();
		VgiOperationGeneratorOsmRelationImpl opGenerator = new VgiOperationGeneratorOsmRelationImpl(factory);
		
		List<IVgiOperation> operations = opGenerator.generateRelationOperations(r2, r1);
		
		Assert.assertEquals(1, operations.size());
		
		Assert.assertEquals(VgiOperationType.OP_REORDER_MEMBER, operations.get(0).getVgiOperationType());
		Assert.assertEquals(1, operations.get(0).getPosition());
		Assert.assertEquals(2, operations.get(0).getRefId());
	}
}
