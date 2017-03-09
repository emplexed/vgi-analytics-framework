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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline;

import java.io.File;
import java.util.List;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.IVgiPipelineConsumer;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.producer.IVgiAnalysisPipelineProducer;
import gnu.trove.list.array.TLongArrayList;

/**
 * Interface to controls VGI pipelines. It receives VGI data from a VGI data producer and
 * sends the VGI features to one or more VGI consumers. The VGI pipeline
 * supports multi-threading.
 *
 */
public interface IVgiPipeline {

	void start();

	void setConsumers(List<IVgiPipelineConsumer> consumers);
	List<IVgiPipelineConsumer> getConsumers();

	void setProducer(IVgiAnalysisPipelineProducer producer);
	void setProducerQuadtree(IVgiAnalysisPipelineProducer producerQuadtree);
	
	void setPbfDataFolder(File file);
	void setNumThreads(int threadCount);
	
	/**
	 * Sets a node ID filter. Set an empty list if no ways should be processed.
	 * Skip this method if all nodes should be processed.
	 * @param filterNodeId list of node IDs
	 */
	void setFilterNodeId(TLongArrayList filterNodeId);
	
	/**
	 * Sets a way ID filter. Set an empty list if no ways should be processed.
	 * Skip this method if all ways should be processed.
	 * @param filterWayId list of way IDs
	 */
	void setFilterWayId(TLongArrayList filterWayId);
	
	/**
	 * Sets a relation ID filter. Set an empty list if no relations should be processed.
	 * Skip this method if all relations should be processed.
	 * @param filterRelationId list of relation IDs
	 */
	void setFilterRelationId(TLongArrayList filterRelationId);

	void setFilterGeometryType(VgiGeometryType filterGeometryType);
	
	void setConstrainedFilter(boolean constrainedFilter);

	void setFilterFileId(int filterFileId);

	void setCoordinateOnly(boolean coordinatesOnly);
}
