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

package at.salzburgresearch.vgi.vgianalyticsframework.application;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.importer.OsmDataConsumer;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.importer.impl.VgiOperationGeneratorDataHandlerImpl;
import junit.framework.Assert;

/**
 * JUnit Test class
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/application-context-osm-op-generator.xml"})
public class TestApplicationOsmHistoryImporter {

	@Autowired
	private ApplicationContext ctx;
	
	@Test
    public void testApplicationOsmHistoryImporter() {
		IVgiPipelineSettings settings = ((IVgiPipelineSettings)ctx.getBean("vgiPipelineSettings"));
		OsmDataConsumer osmDataConsumer = ((VgiOperationGeneratorDataHandlerImpl)ctx.getBean("vgiOperationGeneratorDataHandler"));
		
		Assert.assertNotNull(settings);
		Assert.assertNotNull(osmDataConsumer);
	}
}
