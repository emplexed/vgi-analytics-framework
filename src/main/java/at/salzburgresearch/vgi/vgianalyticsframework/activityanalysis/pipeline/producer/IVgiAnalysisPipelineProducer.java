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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.producer;

import gnu.trove.TLongArrayList;

import java.io.File;
import java.util.concurrent.BlockingQueue;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;

public interface IVgiAnalysisPipelineProducer extends Runnable {

	void setQueue(BlockingQueue<IVgiFeature> queue);

	void setPbfDataFolder(File pbfDataFolder);

	void setProducerCount(int producerCount);
	void setProducerNumber(int producerNumber);

	void setStartFileId(int startFileId);

	void setFilterNodeId(TLongArrayList filterNodeId);
	void setFilterWayId(TLongArrayList filterWayId);
	void setFilterRelationId(TLongArrayList filterRelationId);
	void setFilterGeometryType(VgiGeometryType filterGeometryType);

	void setConstrainedFilter(boolean constrainedFilter);
	void setFilterFileId(int filterFileId);

	void setCoordinateOnly(boolean coordinatesOnly);


}
