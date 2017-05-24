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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.impl;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiActionDefinitionRule;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.impl.VgiOperationPbfReaderImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.impl.VgiOperationPbfReaderQuadtreeImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipeline;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.IVgiPipelineConsumer;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.producer.IVgiAnalysisPipelineProducer;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;
import gnu.trove.list.array.TLongArrayList;

/**
 * Controls VGI pipelines. It receives VGI data from a VGI data producer and
 * sends the VGI features to one or more VGI consumers. The VGI pipeline
 * supports multi-threading.
 *
 */
public class VgiPipelineImpl implements IVgiPipeline {
	private static Logger log = org.apache.logging.log4j.LogManager.getLogger(VgiPipelineImpl.class);
	
	private IVgiPipelineSettings settings;

	private List<IVgiPipelineConsumer> consumers;

	private int queueSize = 10000;
	private int batchSize = 10000;
	
	private IVgiAnalysisPipelineProducer producer = null;
	private IVgiAnalysisPipelineProducer producerQuadtree = null;
	private int numThreads = 1;
	
	private File pbfDataFolder = null;
	private TLongArrayList filterNodeId = null;
	private TLongArrayList filterWayId = null;
	private TLongArrayList filterRelationId = null;
	private VgiGeometryType filterGeometryType = VgiGeometryType.UNDEFINED;
	private boolean constrainedFilter = false;
	private int filterFileId = -1;
	private boolean coordinateOnly = false;

	private Date timerStart = null;
	private SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat dateFormatOSM = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	
	public VgiPipelineImpl(IVgiPipelineSettings settings) {
		this.settings = settings;
	}
	
	/**
	 * Starts the pipeline. Create producers, lets them read the VGI operations, receives the data and sends the operations to a set of consumers
	 */
	@Override
	public void start() {
		timerStart = new Date();
		
		BlockingQueue<IVgiFeature> queue = new ArrayBlockingQueue<IVgiFeature>(queueSize);
		
		/** Create thread(s) which will read the PBF files */
		Thread[] producerThread = new Thread[numThreads];
		
		for (int i=0; i<numThreads; i++) {
			producer = (settings.isReadQuadtree() && producerQuadtree != null) ? new VgiOperationPbfReaderQuadtreeImpl(settings) : new VgiOperationPbfReaderImpl(settings);
			producer.setQueue(queue);
			producerThread[i] = new Thread(producer);
			
			producer.setProducerCount(numThreads);
			producer.setProducerNumber(i);
			producer.setPbfDataFolder((pbfDataFolder != null) ? pbfDataFolder : settings.getPbfDataFolder());
			
			producer.setFilterNodeId(filterNodeId);
			producer.setFilterWayId(filterWayId);
			producer.setFilterRelationId(filterRelationId);
			producer.setFilterGeometryType(filterGeometryType);
			producer.setConstrainedFilter(constrainedFilter);
			producer.setCoordinateOnly(coordinateOnly);
			
			producer.setFilterFileId(filterFileId);
			
			producerThread[i].start();
		}
		
		List<IVgiFeature> currentBatch = new ArrayList<IVgiFeature>();
		
		try {
			doBeforeFirstBatch();
			
			/** Read queue as long as it is not empty */
			boolean resumePipeline = true;
			while (resumePipeline) {
				resumePipeline = false;
				for (int i=0; i<numThreads; i++) {
					if (producerThread[i].isAlive()) resumePipeline = true;
				}
				if (!queue.isEmpty()) resumePipeline = true;
				if (!resumePipeline) break;
				
				IVgiFeature currentFeature = queue.poll(60, TimeUnit.MILLISECONDS);
				
				if (currentFeature == null) continue;
				
				/** Detach batch if minimum batch size is reached */
				if (currentBatch.size() >= batchSize) {
					detachBatch(currentBatch);
					currentBatch.clear();
				}

				currentBatch.add(currentFeature);
			}
			
			if (currentBatch.size() > 0) {
				detachBatch(currentBatch);
			}

			doAfterLastBatch();

		} catch (InterruptedException e) {
			log.error("error joining producer thread", e);
		}
		
		if (settings.getActionAnalyzerList() != null 
				|| settings.getOperationAnalyzerList() != null 
				|| settings.getFeatureAnalyzerList() != null) {
			writeMetaData(settings.getResultFolder());			
		}
	}
	
	/**
	 * Will be done before first batch arrives
	 */
	private void doBeforeFirstBatch() {
		if (consumers != null) {
			for (IVgiPipelineConsumer consumer : consumers) {
				consumer.doBeforeFirstBatch();
			}
		}
	}
	
	/**
	 * Detach batches to consumers
	 * @param batch
	 */
	private void detachBatch(List<IVgiFeature> batch) {
		if (consumers != null) {
			for (IVgiPipelineConsumer consumer : consumers) {
				consumer.handleBatch(new ArrayList<IVgiFeature>(batch));
			}
		}
	}
	
	/**
	 * Will be done after last batch has been handled
	 */
	private void doAfterLastBatch() {
		if (consumers != null) {
			for (IVgiPipelineConsumer consumer : consumers) {
				consumer.doAfterLastBatch();
			}
		}
	}
	
	/** 
	 * Writes analysis meta data (settings, ...)
	 * @param path
	 */
	public void writeMetaData(File path) {
		try (CSVFileWriter writer = new CSVFileWriter(path + "/metadata.txt")) {
			writer.writeLine("Setting Profile: " + (settings.getSettingName()));
			writer.writeLine("Analysis start: " + (dateTimeFormat.format(timerStart)));
			writer.writeLine("Analysis end: " + (dateTimeFormat.format(new Date())));
			writer.writeLine("Analysis duration: " + (new Date().getTime() - timerStart.getTime()) + " ms");
			
			writer.writeLine("pbfDataFolder: " + settings.getPbfDataFolder());
			writer.writeLine("useQuadtree: " + settings.isReadQuadtree());
			writer.writeLine("resultFolder: " + settings.getResultFolder());
			
			writer.writeLine("filterUid: " + settings.getFilterUid());
			writer.writeLine("filterTimestamp: " + dateFormatOSM.format(settings.getFilterTimestamp()));
			writer.writeLine("filterElementType: " + settings.getFilterElementType());
			writer.writeLine("filterTag: " + settings.getFilterTag());
			
			writer.writeLine("Analyses:");
			for (IVgiAnalysisAction analysis : settings.getActionAnalyzerList()) {
				writer.writeLine(" - " + analysis.toString() + " (" + analysis.getProcessingTime() + "ms)");
			}
			for (IVgiAnalysisOperation analysis : settings.getOperationAnalyzerList()) {
				writer.writeLine(" - " + analysis.toString() + " (" + analysis.getProcessingTime() + "ms)");
			}
			for (IVgiAnalysisFeature analysis : settings.getFeatureAnalyzerList()) {
				writer.writeLine(" - " + analysis.toString() + " (" + analysis.getProcessingTime() + "ms)");
			}
			writer.writeLine("analysisStartDate: " + dateFormatOSM.format(settings.getAnalysisStartDate()));
			writer.writeLine("analysisEndDate: " + dateFormatOSM.format(settings.getAnalysisEndDate()));
			writer.writeLine("temporalResolution: " + settings.getTemporalResolution());
			writer.writeLine("Feature Types:");
			for (String featureTypeKey : settings.getFeatureTypeList().keySet()) {
				writer.writeLine(" - " + featureTypeKey);
			}
			writer.writeLine("ignoreFeaturesWithoutTags: " + settings.isIgnoreFeaturesWithoutTags());
			writer.writeLine("findRelatedOperations: " + settings.isFindRelatedOperations());
			writer.writeLine("actionTimeBuffer: " + settings.getActionTimeBuffer());
			writer.writeLine("actionDefinitions: ");
			for (IVgiAction action : settings.getActionDefinitionList()) {
				writer.writeLine(" - " + action.getActionName() + " (" + action.getGeometryType() + ")");
				for (VgiActionDefinitionRule rule : action.getDefinition()) {
					writer.writeLine(" - - " + rule.getVgiOperationType() + " (" + rule.getEntryPoint() + ")");
				}
			}
			if (settings.getCurrentPolygon() != null) {
				writer.writeLine("Test Area Label: " + settings.getCurrentPolygon().getLabel());
				writer.writeLine("Test Area Polygon: " + settings.getCurrentPolygon().getPolygon().toText());
			} else {
				writer.writeLine("Test Area Polygon: (no polygon set)");
			}
		} catch (IOException e) {
			log.error("Error while writing CSV file", e);
		}
	}
	
	public int getQueueSize() {
		return queueSize;
	}
	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	public int getBatchSize() {
		return batchSize;
	}
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	@Override
	public List<IVgiPipelineConsumer> getConsumers() {
		return consumers;
	}
	@Override
	public void setConsumers(List<IVgiPipelineConsumer> consumers) {
		this.consumers = consumers;
	}

	@Override
	public void setNumThreads(int threadCount) {
		if (threadCount < 1) {
			throw new IllegalArgumentException("numThreads less than 1");
		}
		this.numThreads = threadCount;
	}

	@Override
	public void setPbfDataFolder(File pbfDataFolder) {
		this.pbfDataFolder = pbfDataFolder;
	}
	
	@Override
	public void setProducer(IVgiAnalysisPipelineProducer producer) {
		this.producer = producer;
	}
	
	@Override
	public void setProducerQuadtree(IVgiAnalysisPipelineProducer producerQuadtree) {
		this.producerQuadtree = producerQuadtree;
	}

	@Override
	public void setFilterNodeId(TLongArrayList filterNodeId) {
		this.filterNodeId = filterNodeId;
	}

	@Override
	public void setFilterWayId(TLongArrayList filterWayId) {
		this.filterWayId = filterWayId;
	}

	@Override
	public void setFilterRelationId(TLongArrayList filterRelationId) {
		this.filterRelationId = filterRelationId;
	}

	@Override
	public void setFilterGeometryType(VgiGeometryType filterGeometryType) {
		this.filterGeometryType = filterGeometryType;
	}
	
	@Override
	public void setConstrainedFilter(boolean constrainedFilter) {
		this.constrainedFilter = constrainedFilter;
	}
	
	@Override
	public void setFilterFileId(int filterFileId) {
		this.filterFileId = filterFileId;
	}

	@Override
	public void setCoordinateOnly(boolean coordinatesOnly) {
		this.coordinateOnly = coordinatesOnly;
	}
}
