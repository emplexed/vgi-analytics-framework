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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.impl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.vividsolutions.jts.geom.Envelope;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiFeatureImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiFeatureImpl.LocalizeType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationFileListProto.PbfOperationFileList;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationFileListProto.PbfOperationFileList.ElementType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationFileListProto.PbfOperationFileList.PbfOperationFile;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationProto.PbfVgiOperationContainer.PbfVgiFeatureBatch;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationProto.PbfVgiOperationContainer.PbfVgiFeatureBytes;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationProto.PbfVgiOperationContainer.PbfVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.producer.IVgiAnalysisPipelineProducer;

import gnu.trove.list.array.TLongArrayList;

public class VgiOperationPbfReaderImpl implements IVgiAnalysisPipelineProducer {
	private static Logger log = org.apache.logging.log4j.LogManager.getLogger(VgiOperationPbfReaderImpl.class);
	
	protected IVgiPipelineSettings settings = null;
	
	private File pbfDataFolder = null;
	private int startFileId = 1;
	private int producerCount = 1;
	private int producerNumber = 0;
	
	private byte[] byteArray = new byte[0];
	private int byteArrayPointer = 0;
	
	private VgiGeometryType filterGeometryType = VgiGeometryType.UNDEFINED;
	private TLongArrayList filterNodeId = null;
	private TLongArrayList filterWayId = null;
	private TLongArrayList filterRelationId = null;
	private int filterIdPointer = 0;
	
	/** If constrained filter is activated, the PBF file will be processed only if it
	 *  stores enough wanted features (MIN_NODE|WAY_COUNT); Skipped features will be 
	 *  processed later. This feature improves performance during building the quadtree */
	private boolean constrainedFilter = false;
	private static final int MIN_NODE_COUNT = 150;
	private static final int MIN_WAY_COUNT = 10;
	
	private boolean coordinateOnly = false;
	
	private int filterFileId = -1;
	
	/** if true, QT quadrant is WITHIN the filter polygon */
	protected LocalizeType localizeType = LocalizeType.UNDEFINED;
	/** used to cache features during quadtree read */
	protected String cacheIdentifier = "";

	/** queue operations for further processing */
	private BlockingQueue<IVgiFeature> queue;
	
	/** if 2 subsequent PBF operations have same attribute value, only the 1st PBF operation saves the attribute (this reduces PBF file size)  */
	private IVgiOperation currentOperationValues = null;
	private long currentPbfOperationOid = 0l;
	private int currentPbfOperationTimestamp = 0;
	private int currentPbfOperationChangeset = 0;
	private long currentPbfOperationRefId = 0l;
	private int currentPbfOperationLongitude = 0;
	private int currentPbfOperationLatitude = 0;
	
	public VgiOperationPbfReaderImpl(IVgiPipelineSettings settings) {
		this.settings = settings;
	}
	
	@Override
	public void run() {
		if (pbfDataFolder == null) pbfDataFolder = settings.getPbfDataFolder();
		
		log.info("Start reading PBF files");
		log.info(" - Data folder: " + pbfDataFolder);
		if (constrainedFilter) log.info(" - Constrained Filter: {}", constrainedFilter);
		if (filterNodeId != null) log.info(" - Node ID Filter Size: {}", filterNodeId.size());
		if (filterWayId != null) log.info(" - Way ID Filter Size: {}", filterWayId.size());
		if (filterRelationId != null) log.info(" - Relation ID Filter Size: {}", filterRelationId.size());
		if (filterFileId != -1) log.info(" - filterFileId: {}", filterFileId);
		if (producerCount != 1) log.info(" - producer: {} of {}", producerNumber, producerCount);
		if (!filterGeometryType.equals(VgiGeometryType.UNDEFINED)) log.info(" - filterGeometryType: {}", filterGeometryType);
		
		readPbfFiles(false);
	}
	
	protected void readPbfFiles(boolean keepInCache) {
		
		if (keepInCache) {
			if (!settings.getCache().containsKey(cacheIdentifier)) settings.getCache().put(cacheIdentifier, new ArrayList<IVgiFeature>());
		}
		
		/** Read the operation file list */
		PbfOperationFileList pbfFileList = null;
		
		File listFile = new File(pbfDataFolder + File.separator + "operationFileList.pbf");
		
		if (!listFile.exists()) {
			log.error("'{}' not found!", listFile);
			return;
		}
		
		try (FileInputStream fisFileList = new FileInputStream(listFile)) {
			pbfFileList = PbfOperationFileList.parseFrom(fisFileList);
		} catch (IOException e) {
			log.error("IOException while reading PBF files");
		}
		
		/** Reset pointers to 0 */
		filterIdPointer = 0;
		
		/** Iterate through operation files */
		/** Determine first and last file id (related to multi-threading) */
		int firstFileId = startFileId + (int)Math.floor((double)(pbfFileList.getNodeOperationFileCount()-startFileId) / producerCount * producerNumber) + 1;
		if (producerNumber == 0) firstFileId = startFileId;
		int lastFileId = startFileId + (int)Math.floor((double)(pbfFileList.getNodeOperationFileCount()-startFileId) / producerCount * (producerNumber+1));
		log.info("[{}] firstFileId={} lastFileId={}", producerNumber, firstFileId, lastFileId);
		if (settings.getFilterElementType().equals(VgiGeometryType.UNDEFINED) || settings.getFilterElementType().equals(VgiGeometryType.POINT)) {
			read(pbfFileList.getNodeOperationFileList(), keepInCache, firstFileId, lastFileId);
		}

		filterIdPointer = 0;
		firstFileId = startFileId + (int)Math.floor((double)(pbfFileList.getWayOperationFileCount()-startFileId) / producerCount * producerNumber) + 1;
		if (producerNumber == 0) firstFileId = startFileId;
		lastFileId = startFileId + (int)Math.floor((double)(pbfFileList.getWayOperationFileCount()-startFileId) / producerCount * (producerNumber+1));
		if (settings.getFilterElementType().equals(VgiGeometryType.UNDEFINED) || settings.getFilterElementType().equals(VgiGeometryType.LINE)) {
			read(pbfFileList.getWayOperationFileList(), keepInCache, firstFileId, lastFileId);
		}

		filterIdPointer = 0;
		firstFileId = startFileId + (int)Math.floor((double)(pbfFileList.getRelationOperationFileCount()-startFileId) / producerCount * producerNumber) + 1;
		if (producerNumber == 0) firstFileId = startFileId;
		lastFileId = startFileId + (int)Math.floor((double)(pbfFileList.getRelationOperationFileCount()-startFileId) / producerCount * (producerNumber+1));
		if (settings.getFilterElementType().equals(VgiGeometryType.UNDEFINED) || settings.getFilterElementType().equals(VgiGeometryType.RELATION)) {
			read(pbfFileList.getRelationOperationFileList(), keepInCache, firstFileId, lastFileId);
		}
	}
	
	private void read(List<PbfOperationFile> pbfFileList, boolean keepInCache, int firstFileId, int lastFileId) {
		
		/** Iterate through operation files */
		for (PbfOperationFile file : pbfFileList) {
			
			if (filterFileId != -1) {
				if (file.getOperationFileId() != filterFileId) continue;
			}
			
//			if (startFileId > 1) {
//				if (filterGeometryType == VgiGeometryType.UNDEFINED) {
//					if (file.getOperationFileId() < startFileId) continue;
//				} else {
//					if (file.getOperationFileId() < startFileId && file.getElementType().equals(filterElementType)) continue;
//				}
//			}
			
			/** Skip files if multi-threading */
			if (file.getOperationFileId() < firstFileId || file.getOperationFileId() > lastFileId) continue;

			/** Does file exist? */
			String elementTypePrefix = "d";
			VgiGeometryType geometryType = VgiGeometryType.UNDEFINED;
			switch (file.getElementType()) {
			case NODE:
				elementTypePrefix = "n";
				geometryType = VgiGeometryType.POINT;
				break;
			case WAY: 
				elementTypePrefix = "w";
				geometryType = VgiGeometryType.LINE;
				break;
			case RELATION: 
				elementTypePrefix = "r";
				geometryType = VgiGeometryType.RELATION;
				break;
			default: break;
			}
			
			if (!containsRequestedOperations(file, geometryType)) continue;
			
			File pbfFile = new File(pbfDataFolder + "/operation_" + elementTypePrefix + "_" + file.getOperationFileId() + ".pbf");
			
			if (!pbfFile.exists()) {
				log.error("'{}' not found!", pbfFile);
				continue;
			}
			
//			log.info("[{}: Pbf {}_{}/{}] Start: NumOps={}; File={}", producerNumber, elementTypePrefix, file.getOperationFileId(), pbfFileList.size(), file.getNumEntries(), pbfFile);
			/** Initialize variables */
			currentOperationValues = new VgiOperationImpl();
			currentPbfOperationOid = 0l;
			currentPbfOperationTimestamp = 0;
			currentPbfOperationChangeset = 0;
			currentPbfOperationRefId = 0l;
			currentPbfOperationLongitude = 0;
			currentPbfOperationLatitude = 0;

			byteArray = new byte[0];
			byteArrayPointer = 0;
			
			/** Stream and read the operations */
			try (BufferedInputStream bis = new BufferedInputStream(
					new FileInputStream(pbfFile), 1024*64)) {
				
				byte[] byteArray = null;
				PbfVgiFeatureBatch pbfFeature = null;
				
				while (true) {
					
					/** Parse start information (at least 4 bytes) */
					byteArray = readBytes(bis, 3);
					if (byteArray == null) break; /** no more features in this file */
					byte previousByte = 0;
					do { /** read bytes until the sequence 10-5 occurs */
						previousByte = byteArray[byteArray.length-1];
						byteArray = readBytes(bis, 1);
					} while (previousByte != 10 || byteArray[0] != 5);
					
					/** Parse PBF operation's length in bytes (5 bytes) */
					byteArray = readBytes(bis, 5);
					PbfVgiFeatureBytes b = PbfVgiFeatureBytes.parseFrom(byteArray);
					
					/** Parse other information (at least 2 bytes, until positive integer) */
					byteArray = readBytes(bis, 1);
					do {
						byteArray = readBytes(bis, 1);
					} while (byteArray[0] < 0);
					
					/** Parse Operation (x bytes) */
					byteArray = readBytes(bis, b.getBytes());
					
					pbfFeature = PbfVgiFeatureBatch.parseFrom(byteArray);
					
					/** Deserialize operation */
					List<IVgiFeature> featureList = deserializeFeature(pbfFeature, geometryType);
					
					/** Enqueue operation (feature can be null if filter is applied) */
					for (IVgiFeature feature : featureList) {
						feature.setLocalizeType(localizeType);
						enqueueFeature(feature);
						if (keepInCache) settings.getCache().get(cacheIdentifier).add(feature);
					}
					
					/** No more filter values in this file */
					if (filterNodeId != null && file.getElementType().equals(ElementType.NODE) && (filterIdPointer == filterNodeId.size() || file.getMaxElementId() < filterNodeId.get(filterIdPointer))) break;
					if (filterWayId != null && file.getElementType().equals(ElementType.WAY) && (filterIdPointer == filterWayId.size() || file.getMaxElementId() < filterWayId.get(filterIdPointer))) break;
				}
				
			} catch (InvalidProtocolBufferException e) {
				log.error("Operation after: {}/{} > {}, {}", currentOperationValues.getVgiGeometryType(), currentOperationValues.getOid(), currentOperationValues.getVgiOperationType(), currentOperationValues.getTimestamp());
				e.printStackTrace();
				/** http://www.openstreetmap.org/browse/changeset/14246617 */
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				log.error("'{}' not found!", pbfFile);
			}
			
//			/** log progress of reading PBFs */
//			log.info("[{}: Pbf {}_{}/{}] End  : NumOps={} {}", producerNumber, elementTypePrefix, file.getOperationFileId(), pbfFileList.size(), file.getNumEntries(), ((numDuration > 0) ? "; Dur=" + (duration/numDuration) : ""));
		}
//		log.info("[{}] Reading PBF files finished", producerNumber);
    }
	
	/**
	 * checks if file contains requested content
	 * @param file
	 * @return true if the file contains requested content
	 */
	private boolean containsRequestedOperations(PbfOperationFile file, VgiGeometryType geometryType) {
		/** Geometry type filter */
		if (!filterGeometryType.equals(VgiGeometryType.UNDEFINED) && !geometryType.equals(filterGeometryType)) return false;
		
		int constrainedFilterNodePointer = 0; //TODO start with current pointer instead of 0
		int constrainedFilterWayPointer = 0;
		int constrainedFilterRelationPointer = 0;
		
		/** is current node/way/relation id in filter list? */
		/** If filter is active, check min/max values of this file if there are enough (MIN_NODE_COUNT or 0) features in this file */
		if (geometryType.equals(VgiGeometryType.POINT) && filterNodeId != null) { //TODO merge node/way/relation logic
			
			/** Skip features with ID less than MinNodeId */
			while (constrainedFilterNodePointer < filterNodeId.size() && filterNodeId.get(constrainedFilterNodePointer) < file.getMinElementId()) {
				constrainedFilterNodePointer++;
			}
			/** Count features between MinElementId and MaxElementId */
			int count = 0;
			while ((constrainedFilterNodePointer < filterNodeId.size()) && (filterNodeId.get(constrainedFilterNodePointer) <= file.getMaxElementId())) {
				count++;
				constrainedFilterNodePointer++;
				if (count > ((constrainedFilter) ? MIN_NODE_COUNT : 0)) return true;
			}
			
		} else if (geometryType.equals(VgiGeometryType.LINE) && filterWayId != null) {
				
			/** Is one of the filtered feature IDs in this PBF file? */
			while (constrainedFilterWayPointer < filterWayId.size() && filterWayId.get(constrainedFilterWayPointer) < file.getMinElementId()) {
				constrainedFilterWayPointer++;
			}
			int count = 0;
			while ((constrainedFilterWayPointer < filterWayId.size()) && (filterWayId.get(constrainedFilterWayPointer) <= file.getMaxElementId())) {
				count++;
				constrainedFilterWayPointer++;
				if (count > ((constrainedFilter) ? MIN_WAY_COUNT : 0)) return true;
			}
			
		} else if (geometryType.equals(VgiGeometryType.RELATION) && filterRelationId != null) {
			/** Is one of the filtered feature IDs in this PBF file? */
			while (constrainedFilterRelationPointer < filterRelationId.size() && filterRelationId.get(constrainedFilterRelationPointer) < file.getMinElementId()) {
				constrainedFilterRelationPointer++;
			}
			int count = 0;
			while ((constrainedFilterRelationPointer < filterRelationId.size()) && (filterRelationId.get(constrainedFilterRelationPointer) <= file.getMaxElementId())) {
				count++;
				constrainedFilterRelationPointer++;
				if (count > 0) return true;
			}
			
		} else {
			return true; /** No element id filter */
		}
		
		return false;
	}
    
	private byte[] readBytes(BufferedInputStream bis, int numBytes) throws IOException {

    	byte[] bytes = new byte[numBytes];
		
		for (int i=0; i<numBytes; i++) {
			if (byteArrayPointer == byteArray.length) {
				readBytesFromFile(bis);
				if (byteArray == null) return null;
				byteArrayPointer = 0;
			}
			bytes[i] = byteArray[byteArrayPointer++];
		}
		
		return bytes;
    }
    
	private void readBytesFromFile(BufferedInputStream bis) throws IOException {
    	int numBytes = 1024*16;
    	byteArray = new byte[numBytes];
    	
		int count = bis.read(byteArray, 0, numBytes);
		if (count <= 0){
			byteArray = null;
		} else if (count < numBytes) {
	    	byte[] byteArrayNew = new byte[count];
	    	for (int i=0; i<count; i++) {
	    		byteArrayNew[i] = byteArray[i];
	    	}
	    	byteArray = byteArrayNew;
		}
    }
    
    /**
     * Offers feature to queue
     * @param feature : list of type IVgiOperation
     */
	protected void enqueueFeature(IVgiFeature feature) {
		if (feature.getOperationList().size() == 0) {
			log.warn("Feature {} without operations", feature.getOid());
			return;
		}
		
		try {
			boolean enqueued = false;
			while (!enqueued) {
				enqueued = queue.offer(feature, 50, TimeUnit.MILLISECONDS);
			}
		} catch (InterruptedException e) {
			log.error("interrupted while offering data to queue", e);
		}
	}
    
	/**
	 * Reads an operation from PBF file
	 * @param PBF Operation
	 * @return VGI Operation
	 */
    private List<IVgiFeature> deserializeFeature(PbfVgiFeatureBatch pbfFeatureBatch, VgiGeometryType geometryType) {
    	List<IVgiFeature> featureList = new ArrayList<IVgiFeature>();
    	IVgiFeature feature = null;
    	IVgiFeature skipFeature = null;
    	
    	for (PbfVgiOperation pbfOperation : pbfFeatureBatch.getOperationList()) {
	    	/** id (offset, all) */
    		if (feature == null || feature.getOid() != pbfOperation.getOid() + currentPbfOperationOid) {
    			if (feature != null && feature.getOperationList().size() > 0) {
    				featureList.add(feature);
    			}
    			
    			feature = new VgiFeatureImpl();
            	feature.setBBox(new Envelope());
            	feature.setOid(pbfOperation.getOid() + currentPbfOperationOid);
            	currentPbfOperationOid = feature.getOid();
            	feature.setVgiGeometryType(geometryType);
    		}
    		
    		if (feature.equals(skipFeature) || !filterByElementId(feature)) {
    			skipFeature = feature;
    			feature = null;
    		}
    		
	    	IVgiOperation operation = new VgiOperationImpl();
	    	
			/** op type (transform, all) */
			if (pbfOperation.hasVgiOperationType()) {
				currentOperationValues
						.setVgiOperationType(VgiOperationType.getOperationTypeById(pbfOperation.getVgiOperationType()));
			}
	    	if (!coordinateOnly) {
				/** user id (1:1, all) */
	    		if (pbfOperation.hasUid()) {
	    			currentOperationValues.setUid(pbfOperation.getUid());
	    		}
	    	}
			/** timestamp (offset and transform, all) */
			if (pbfOperation.hasTimestamp()) {
				currentPbfOperationTimestamp += pbfOperation.getTimestamp();
				currentOperationValues.setTimestamp(new Date(
						(long) (currentPbfOperationTimestamp + VgiOperationPbfWriterImpl.TIMESTAMP_OFFSET) * 1000));
			}
		    if (!coordinateOnly) {
				/** changeset id (offset, all) */
				if (pbfOperation.hasChangesetId()) {
					currentPbfOperationChangeset += pbfOperation.getChangesetId();
				}
				/** version (1:1, all) */
	    		if (pbfOperation.hasVersion()) {
	    			currentOperationValues.setVersion((short) pbfOperation.getVersion());
	    		}
		    }
	    	if (feature != null) {
		    	operation.setOid(feature.getOid());
		    	operation.setVgiGeometryType(feature.getVgiGeometryType());
				operation.setVgiOperationType(currentOperationValues.getVgiOperationType());
    			operation.setUid(currentOperationValues.getUid());
				operation.setTimestamp(currentOperationValues.getTimestamp());
				operation.setChangesetid(currentPbfOperationChangeset);
    			operation.setVersion(currentOperationValues.getVersion());
    			if (operation.getTimestamp().before(settings.getFilterTimestamp())) {
    				feature.addOperation(operation);
    			}
			}
		    if (!coordinateOnly) {
				/** ref id (offset, if hasRef) */
				if (pbfOperation.hasRef()) {
					currentPbfOperationRefId += pbfOperation.getRef();
					if (feature != null) {
						operation.setRefId(currentPbfOperationRefId);
					}
				}
				if (feature != null) {
					/** key (1:1, if hasKey) */
					if (pbfOperation.hasKey()) {
						operation.setKey(pbfOperation.getKey());
					}
					/** value (1:1, if hasValue) */
					if (pbfOperation.hasValue()) {
						operation.setValue(pbfOperation.getValue());
					}
					/** position (1:1, if hasPosition) */
					if (pbfOperation.hasPosition()) {
						operation.setPosition(pbfOperation.getPosition());
					}
				}
	    	}
			/** coordinate (offset, if hasLongitude) */
			if (pbfOperation.hasLongitude()) {
				currentPbfOperationLongitude += pbfOperation.getLongitude();
				currentPbfOperationLatitude += pbfOperation.getLatitude();
				if (feature != null) {
					operation.setCoordinateFromInteger(currentPbfOperationLongitude, currentPbfOperationLatitude);
					feature.getBBox().expandToInclude(operation.getCoordinate());
				}
			}
    	}
    	
		if (feature != null && feature.getOperationList().size() > 0) {
			featureList.add(feature);
		}
    	
		return featureList;
    }
	
	/**
	 * Filters feature by element ID and element type
	 * 
	 * @param feature
	 * @return TRUE if no filter is set or if featureId is included in filter.
	 *         FALSE if filter is set and featureId is not included in filter
	 */
	private boolean filterByElementId(IVgiFeature feature) {
		TLongArrayList filterList = new TLongArrayList();
		if (feature.getVgiGeometryType().equals(VgiGeometryType.POINT)) {
			filterList = filterNodeId;
		} else if (feature.getVgiGeometryType().equals(VgiGeometryType.LINE)) {
			filterList = filterWayId;
		} else if (feature.getVgiGeometryType().equals(VgiGeometryType.RELATION)) {
			filterList = filterRelationId;
		}

		if (filterList == null) {
			return true;
		}

		do {
			if (filterIdPointer == filterList.size()) {
				/** End of filter list */
				return false;
			} else if (filterList.get(filterIdPointer) == feature.getOid()) {
				/** Feature ID member of filter list > PROCESS this operation */
				return true;
			} else if (filterList.get(filterIdPointer) > feature.getOid()) {
				/** Feature ID NO member of filter list > SKIP this operation */
				return false;
			}
			/** Current Filter ID is too low > NEXT filter feature id */
			filterIdPointer++;
		} while (true);
	}
	
	@Override
	public void setQueue(BlockingQueue<IVgiFeature> queue) {
		this.queue = queue;
	}

	@Override
	public void setPbfDataFolder(File pbfDataFolder) {
		this.pbfDataFolder = pbfDataFolder;
	}

	@Override
	public void setProducerCount(int producerCount) {
		this.producerCount = producerCount;
	}

	@Override
	public void setProducerNumber(int producerNumber) {
		this.producerNumber = producerNumber;
	}

	@Override
	public void setStartFileId(int startFileId) {
		this.startFileId = startFileId;
	}

	@Override
	public void setFilterGeometryType(VgiGeometryType filterGeometryType) {
		this.filterGeometryType = filterGeometryType;
	}

	@Override
	public void setFilterNodeId(TLongArrayList filterNodeId) {
		this.filterNodeId = filterNodeId;
		if (this.filterNodeId != null) this.filterNodeId.sort();
	}

	@Override
	public void setFilterWayId(TLongArrayList filterWayId) {
		this.filterWayId = filterWayId;
		if (this.filterWayId != null) this.filterWayId.sort();
	}

	@Override
	public void setFilterRelationId(TLongArrayList filterRelationId) {
		this.filterRelationId = filterRelationId;
		if (this.filterRelationId != null) this.filterRelationId.sort();
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

	public LocalizeType isLocalizeType() {
		return localizeType;
	}
	public void setLocalizeType(LocalizeType localizeType) {
		this.localizeType = localizeType;
	}
}
