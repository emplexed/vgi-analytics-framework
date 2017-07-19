package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vividsolutions.jts.geom.Coordinate;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiModelFactory;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiFeatureImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.IVgiOperationPbfWriter;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipeline;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.impl.ReadAllFeaturesConsumer;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/application-context-test-vgi-persistence.xml"})
public class TestVgiOperationReaderWriter {
	
	@Autowired
	@Qualifier("vgiModelFactory")
	private IVgiModelFactory operationFactory;
	
	@Autowired
	@Qualifier("vgiOperationPbfWriter")
	private IVgiOperationPbfWriter vgiOperationPbfWriter;
	
	@Autowired
	@Qualifier("vgiPipeline")
	private IVgiPipeline pipeline;
	
	@Rule
	public TemporaryFolder folder= new TemporaryFolder();

	@Test
	public void testReadAndWriteVgiOperations() {
		/** Create and write operation */

		IVgiOperation operation1 = operationFactory.newOperation(8, VgiGeometryType.POINT,
				VgiOperationType.OP_CREATE_NODE, 123, "USER", new Date(500), (short) 1, (int) 345,
				new Coordinate(13.444, 46.777), null, null, -1, -1);
		IVgiOperation operation2 = operationFactory.newOperation(8, VgiGeometryType.POINT,
				VgiOperationType.OP_MODIFY_COORDINATE, 456, "USER", new Date(1000), (short) 2, (int) 567,
				new Coordinate(13.465, 46.789), "k", "v", 11, 12);
		List<IVgiOperation> operations = new ArrayList<IVgiOperation>();
		operations.add(operation1);
		operations.add(operation2);
		IVgiFeature feature = new VgiFeatureImpl(operations);
		feature.setOid(1l);
		feature.setVgiGeometryType(VgiGeometryType.POINT);
		File directory = null;
		try {
			directory = folder.newFolder();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Assert.assertEquals(true, directory.exists());
		
		vgiOperationPbfWriter.initializePbfWriterToAppend(directory);
		vgiOperationPbfWriter.writePbfFeature(feature);
		vgiOperationPbfWriter.terminatePbfWriter();
		
		/** Read operation */
		pipeline.setPbfDataFolder(directory);
		pipeline.setFilterNodeId(null);
		pipeline.setFilterWayId(null);
		pipeline.setFilterRelationId(null);
		pipeline.start();
		
		List<IVgiFeature> featureList = ((ReadAllFeaturesConsumer)pipeline.getConsumers().get(0)).getFeatureList();
		
		Assert.assertEquals(1, featureList.size());
		
		Assert.assertEquals(8, featureList.get(0).getOperationList().get(0).getOid());
		
		IVgiOperation operationRead = featureList.get(0).getOperationList().get(1);
		
		Assert.assertEquals(8, operationRead.getOid());
		Assert.assertEquals(VgiGeometryType.POINT, operationRead.getVgiGeometryType());
		Assert.assertEquals(VgiOperationType.OP_MODIFY_COORDINATE, operationRead.getVgiOperationType());
		Assert.assertEquals(456, operationRead.getUid());
//		Assert.assertEquals("USER", operationRead.getUser());
		Assert.assertEquals(1000, operationRead.getTimestamp().getTime());
		Assert.assertEquals(2, operationRead.getVersion());
		Assert.assertEquals(567, operationRead.getChangesetid());
		Assert.assertEquals(13.465, operationRead.getCoordinate().x, 0.0);
		Assert.assertEquals(46.789, operationRead.getCoordinate().y, 0.0);
		Assert.assertEquals("k", operationRead.getKey());
		Assert.assertEquals("v", operationRead.getValue());
		Assert.assertEquals(11, operationRead.getRefId());
		Assert.assertEquals(12, operationRead.getPosition());
	}
}
