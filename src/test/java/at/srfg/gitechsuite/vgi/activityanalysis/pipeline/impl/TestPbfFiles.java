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

package at.srfg.gitechsuite.vgi.activityanalysis.pipeline.impl;

import gnu.trove.TLongArrayList;

import java.io.File;
import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipeline;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.impl.ReadAllFeaturesConsumer;

/**
 * JUnit Test class<br />
 * Settings can be modified in the test/resources/vgi.properties file
 * @author sgroeche
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/application-context-vgi-pipeline.xml", "classpath:/application-context-test-vgi-pipeline.xml"})
public class TestPbfFiles {
//	private static Logger log = Logger.getLogger(TestPbfFiles.class);
	
	@Autowired
	private ApplicationContext ctx;
	
	@Autowired
	@Qualifier("vgiPipelineSettings")
	private IVgiPipelineSettings settings;
	
	@Autowired
	@Qualifier("vgiPipelineTest")
	private IVgiPipeline pipeline;
	
	@Value("#{testProperties['general.pbfDataFolder']}")
	private String pbfDataFolder = "";
	
	/**
	 * Berlin has to be part of the VGI operation dataset in order to run this test
	 */
	@Test
	@Rollback(false)
    public void testPbfBerlin() throws IOException {
		settings.loadSettings(null);
		settings.setPbfDataFolder(new File(pbfDataFolder));
		settings.setReadQuadtree(false);
		settings.setFilterPolygon(null);
		
		TLongArrayList filterNode = new TLongArrayList();
		filterNode.add(288855825l);
		TLongArrayList filterWay = new TLongArrayList();
		filterWay.add(104393803l);
		pipeline.setFilterNodeId(filterNode);
		pipeline.setFilterWayId(filterWay);
		pipeline.setFilterRelationId(new TLongArrayList());
		pipeline.start();
		
		List<IVgiFeature> features = ctx.getBean("readAllFeaturesConsumer", ReadAllFeaturesConsumer.class).getFeatureList();
		
		Assert.assertEquals(features.size(), 2);
		
		IVgiFeature feature = features.get(0);
		
		Assert.assertEquals(feature.getOid(), 288855825l);
		
		IVgiOperation operation = feature.getOperationList().get(0);
		
		/** https://www.openstreetmap.org/api/0.6/node/288855825/history */
		
		Assert.assertEquals(1218998063000l, operation.getTimestamp().getTime());
		Assert.assertEquals(294637, operation.getChangesetid());
		Assert.assertEquals(10549, operation.getUid());
		Assert.assertEquals(1, operation.getVersion());
		Assert.assertEquals(13.3776302, operation.getCoordinate().x, 0.0000001);
		Assert.assertEquals(52.516103, operation.getCoordinate().y, 0.0000001);
		
		feature = features.get(1);
		
		Assert.assertEquals(feature.getOid(), 104393803l);
		
		operation = feature.getOperationList().get(0);
		
		/** https://www.openstreetmap.org/api/0.6/way/104393803/history */

		Assert.assertEquals(operation.getTimestamp().getTime(), 1300295857000l);
		Assert.assertEquals(operation.getChangesetid(), 7577557);
		Assert.assertEquals(operation.getUid(), 10549);
		
    }
}