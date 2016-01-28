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

import org.apache.log4j.Logger;

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
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationProto.PbfVgiOperationList.PbfVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationProto.PbfVgiOperationList.PbfVgiFeatureBytes;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationProto.PbfVgiOperationList.PbfVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.producer.IVgiAnalysisPipelineProducer;

import gnu.trove.list.array.TLongArrayList;

public class VgiOperationPbfReaderImpl implements IVgiAnalysisPipelineProducer {
	private static Logger log = Logger.getLogger(VgiOperationPbfReaderImpl.class);
	
	protected IVgiPipelineSettings settings = null;
	
	protected File pbfDataFolder = null;
	protected int startFileId = 1;
	protected int producerCount = 1;
	protected int producerNumber = 0;
	
	protected byte[] byteArray = new byte[0];
	protected int byteArrayPointer = 0;
	
	protected VgiGeometryType filterGeometryType = VgiGeometryType.UNDEFINED;
	protected TLongArrayList filterNodeId = null;
	protected int filterNodeIdPointer = 0;
	protected TLongArrayList filterWayId = null;
	protected int filterWayIdPointer = 0;
	protected TLongArrayList filterRelationId = null;
	protected int filterRelationIdPointer = 0;
	
	/** If constrained filter is activated, the PBF file will be processed only if it
	 *  stores enough wanted features (MIN_NODE|WAY_COUNT); Skipped features will be 
	 *  processed later. This feature improves performance during building the quadtree */
	protected boolean constrainedFilter = false;
	protected static final int MIN_NODE_COUNT = 150;
	protected static final int MIN_WAY_COUNT = 10;
	
	protected boolean coordinateOnly = false;
	
	protected int filterFileId = -1;
	
	/** if true, QT quadrant is WITHIN the filter polygon */
	protected LocalizeType localizeType = LocalizeType.UNDEFINED;

	/** queue operations for further processing */
	protected BlockingQueue<IVgiFeature> queue;
	
	/** if 2 subsequent PBF operations have same attribute value, only the 1st PBF operation saves the attribute (this reduces PBF file size)  */
	protected IVgiOperation previousOperationValues = null;
	
	protected String cacheIdentifier = "";
	
	private long previousPbfOperationOid = 0l;
	private int previousPbfOperationTimestamp = 0;
	private int previousPbfOperationChangeset = 0;
	private long previousPbfOperationRefId = 0l;
	private int previousPbfOperationLongitude = 0;
	private int previousPbfOperationLatitude = 0;
	
	public VgiOperationPbfReaderImpl(IVgiPipelineSettings settings) {
		this.settings = settings;
	}
	
	@Override
	public void run() {
		if (pbfDataFolder == null) pbfDataFolder = settings.getPbfDataFolder();
		
		log.info("Start reading PBF files");
		log.info(" - Data folder: " + pbfDataFolder);
		if (constrainedFilter) log.info(" - Constrained Filter: " + constrainedFilter);
		if (filterNodeId != null) log.info(" - Node ID Filter Size: " + filterNodeId.size());
		if (filterWayId != null) log.info(" - Way ID Filter Size: " + filterWayId.size());
		if (filterRelationId != null) log.info(" - Relation ID Filter Size: " + filterRelationId.size());
		if (filterFileId != -1) log.info(" - filterFileId: " + filterFileId);
		if (producerCount != 1) log.info(" - producer: " + producerNumber + " (of " + producerCount + ")");
		if (!filterGeometryType.equals(VgiGeometryType.UNDEFINED)) log.info(" - filterGeometryType: " + filterGeometryType);
		
		readPbfFiles(false);
	}
	
	protected void readPbfFiles(boolean keepInCache) {
		
		if (keepInCache) {
			if (!settings.getCache().containsKey(cacheIdentifier)) settings.getCache().put(cacheIdentifier, new ArrayList<IVgiFeature>());
		}
		
		/** Read the operation file list */
		PbfOperationFileList pbfFileList = null;
		
		if (!new File(pbfDataFolder + File.separator + "operationFileList.pbf").exists()) {
			log.error("'" + pbfDataFolder + File.separator + "operationFileList.pbf' not found!");
			return;
		}
		
		try (FileInputStream fisFileList = new FileInputStream(pbfDataFolder + File.separator + "operationFileList.pbf")) {
			pbfFileList = PbfOperationFileList.parseFrom(fisFileList);
		} catch (IOException e) {
			log.error("IOException while reading PBF files");
		}
		
		/** Reset pointers to 0 */
		filterNodeIdPointer = 0;
		filterWayIdPointer = 0;
		filterRelationIdPointer = 0;
		
		/** Iterate through operation files */
		/** Determine first and last file id (related to multi-threading) */
		int firstFileId = startFileId + (int)Math.floor((double)(pbfFileList.getNodeOperationFileCount()-startFileId) / producerCount * producerNumber) + 1;
		if (producerNumber == 0) firstFileId = startFileId;
		int lastFileId = startFileId + (int)Math.floor((double)(pbfFileList.getNodeOperationFileCount()-startFileId) / producerCount * (producerNumber+1));
		if (settings.getFilterElementType().equals(VgiGeometryType.UNDEFINED) || settings.getFilterElementType().equals(VgiGeometryType.POINT)) {
			read(pbfFileList.getNodeOperationFileList(), keepInCache, firstFileId, lastFileId);
		}
		
		firstFileId = startFileId + (int)Math.floor((double)(pbfFileList.getWayOperationFileCount()-startFileId) / producerCount * producerNumber) + 1;
		if (producerNumber == 0) firstFileId = startFileId;
		lastFileId = startFileId + (int)Math.floor((double)(pbfFileList.getWayOperationFileCount()-startFileId) / producerCount * (producerNumber+1));
		if (settings.getFilterElementType().equals(VgiGeometryType.UNDEFINED) || settings.getFilterElementType().equals(VgiGeometryType.LINE)) {
			read(pbfFileList.getWayOperationFileList(), keepInCache, firstFileId, lastFileId);
		}
		
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
			
//			if (!file.getElementType().equals(ElementType.WAY)) continue;
//			if (file.getOperationFileId() < 20360) continue;
			
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
			
			if (!new File(pbfDataFolder + "/operation_" + elementTypePrefix + "_" + file.getOperationFileId() + ".pbf").exists()) {
				log.error(pbfDataFolder + "/operation_" + elementTypePrefix + "_" + file.getOperationFileId() + ".pbf not found!");
				continue;
			}
			
//			log.info("[" + producerNumber+ ": Pbf " + elementTypePrefix + "_" + file.getOperationFileId() + "/" + pbfFileList.size() + "] Start: NumOps=" + file.getNumEntries() + "; Path=" + pbfDataFolder + "/operation_" + elementTypePrefix + "_" + file.getOperationFileId() + ".pbf");
			
			/** Initialize variables */
			previousOperationValues = new VgiOperationImpl();
			byteArray = new byte[0];
			byteArrayPointer = 0;
			
			previousPbfOperationOid = 0l;
			previousPbfOperationTimestamp = 0;
			previousPbfOperationChangeset = 0;
			previousPbfOperationRefId = 0l;
			previousPbfOperationLongitude = 0;
			previousPbfOperationLatitude = 0;
			
			/** Stream and read the operations */
			try (BufferedInputStream bis = new BufferedInputStream(
					new FileInputStream(pbfDataFolder + "/operation_" + elementTypePrefix + "_" + file.getOperationFileId() + ".pbf"), 1024*64)) {
				
				byte[] byteArray = null;
				PbfVgiFeature pbfFeature = null;
				
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
					
					pbfFeature = PbfVgiFeature.parseFrom(byteArray);
					
					/** Deserialize operation */
					IVgiFeature feature = deserializeFeature(pbfFeature, geometryType);
					
					/** Enqueue operation (feature can be null if filter is applied) */
					if (feature != null) {
						feature.setLocalizeType(localizeType);
						enqueueFeature(feature);
						if (keepInCache) settings.getCache().get(cacheIdentifier).add(feature);
					}
					
					/** No more filter values in this file */
					if (filterNodeId != null && file.getElementType().equals(ElementType.NODE) && (filterNodeIdPointer == filterNodeId.size() || file.getMaxElementId() < filterNodeId.get(filterNodeIdPointer))) break;
					if (filterWayId != null && file.getElementType().equals(ElementType.WAY) && (filterWayIdPointer == filterWayId.size() || file.getMaxElementId() < filterWayId.get(filterWayIdPointer))) break;
				}
				
			    if (bis != null) {
			    	bis.close();
			    }
				
			} catch (InvalidProtocolBufferException e) {
				log.error("Operation after: " + previousOperationValues.getVgiGeometryType() + "/" + previousOperationValues.getOid() + " > " + previousOperationValues.getVgiOperationType() + ", " + previousOperationValues.getTimestamp());
				e.printStackTrace();
				/** http://www.openstreetmap.org/browse/changeset/14246617 */
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				log.error("Operation after: " + previousOperationValues.getVgiGeometryType() + "/" + previousOperationValues.getOid() + " > " + previousOperationValues.getVgiOperationType() + ", " + previousOperationValues.getTimestamp());
				log.warn("End of operation file " + elementTypePrefix + "_" + file.getOperationFileId());// + " after entry " + numEntries + " of " + file.getNumEntries());
			}
			
//			/** log progress of reading PBFs */
//			log.info("[" + producerNumber+ ": Pbf " + elementTypePrefix + "_" + file.getOperationFileId() + "/" + pbfFileList.size() + "] End  : NumOps=" + file.getNumEntries() + ((numDuration > 0) ? "; Dur=" + (duration/numDuration) : ""));
		}
    }
	
	/**
	 * checks if file contains requested content
	 * @param file
	 * @return true if the file contains requested content
	 */
	protected boolean containsRequestedOperations(PbfOperationFile file, VgiGeometryType geometryType) {
		/** Geometry type filter */
		if (!filterGeometryType.equals(VgiGeometryType.UNDEFINED) && !geometryType.equals(filterGeometryType)) return false;
		
		int constrainedFilterNodePointer = 0; //TODO start with current pointer instead of 0
		int constrainedFilterWayPointer = 0;
		int constrainedFilterRelationPointer = 0;
		
		/** is current node/way/relation id in filter list? */
		/** If filter is active, check min/max values of this file if there are enough (MIN_NODE_COUNT or 0) features in this file */
		if (geometryType.equals(VgiGeometryType.POINT) && filterNodeId != null) {
//			if (file.getMinElementId() <= file.getMaxElementId()) {
				
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
//			}
			
		} else if (geometryType.equals(VgiGeometryType.LINE) && filterWayId != null) {
//			if (file.getMinElementId() <= file.getMaxElementId()) {
				
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
//			}
			
		} else if (geometryType.equals(VgiGeometryType.RELATION) && filterRelationId != null) {
			/** Is one of the filtered feature IDs in this PBF file? */
			while (constrainedFilterRelationPointer < filterRelationId.size() && filterRelationId.get(constrainedFilterWayPointer) < file.getMinElementId()) {
				constrainedFilterWayPointer++;
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
	
	/**
	 * Filters feature by element ID and element type
	 * @param feature
	 * @return TRUE if operation oId is included in filter or if no filter applies. FALSE if filter applies and operation oId is not included in filter
	 */
	protected boolean filterByElementId(IVgiFeature feature) {
		if (feature.getVgiGeometryType().equals(VgiGeometryType.POINT)) {
			if (filterNodeId == null) return true;
			
			do {
 				if (filterNodeIdPointer == filterNodeId.size()) {
 					/** End of filter list */
 					return false;
 				} else if (filterNodeId.get(filterNodeIdPointer) == feature.getOid()) {
					/** Feature ID member of filter list > PROCESS this operation */
					return true;
				} else if (filterNodeId.get(filterNodeIdPointer) > feature.getOid()) {
					/** Feature ID NO member of filter list > SKIP this operation */
					return false;
				}
				/** Current Filter ID is too low > NEXT filter feature id */
				filterNodeIdPointer++;
			} while (true);
			
		} else if (feature.getVgiGeometryType().equals(VgiGeometryType.LINE)) {
			if (filterWayId == null) return true;
			
			do {
				if (filterWayIdPointer == filterWayId.size()) {
					/** End of filter list */
					return false;
				} else if (filterWayId.get(filterWayIdPointer) == feature.getOid()) {
					/** Feature ID member of filter list > PROCESS this operation */
					return true;
				} else if (filterWayId.get(filterWayIdPointer) > feature.getOid()) {
					/** Feature ID NO member of filter list > SKIP this operation */
					return false;
				}
				/** Current Filter ID is too low > NEXT filter feature id */
				filterWayIdPointer++;
			} while (true);
			
		} else if(feature.getVgiGeometryType().equals(VgiGeometryType.RELATION)) {
			if (filterRelationId == null) return true;
			
			do {
				if (filterRelationIdPointer == filterRelationId.size()) {
					/** End of filter list */
					return false;
				} else if (filterRelationId.get(filterRelationIdPointer) == feature.getOid()) {
					/** Feature ID member of filter list > PROCESS this operation */
					return true;
				} else if (filterRelationId.get(filterRelationIdPointer) > feature.getOid()) {
					/** Feature ID NO member of filter list > SKIP this operation */
					return false;
				}
				/** Current Filter ID is too low > NEXT filter feature id */
				filterRelationIdPointer++;
			} while (true);
		}
		
		return true;
	}
    
	protected byte[] readBytes(BufferedInputStream bis, int numBytes) {

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
    
	protected void readBytesFromFile(BufferedInputStream bis) {
    	int numBytes = 1024*16;
    	byteArray = new byte[numBytes];
    	
		try {
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
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
    /**
     * Offers feature to queue
     * @param feature : list of type IVgiOperation
     */
	protected void enqueueFeature(IVgiFeature feature) {
		if (feature.getOperationList().size() == 0) {
//			log.warn("Feature without operations");
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
    private IVgiFeature deserializeFeature(PbfVgiFeature pbfFeature, VgiGeometryType geometryType) {
    	IVgiFeature feature = new VgiFeatureImpl();
    	feature.setOid(pbfFeature.getOid() + previousPbfOperationOid);
    	previousPbfOperationOid = feature.getOid();
    	feature.setVgiGeometryType(geometryType);
    	
    	if (!filterByElementId(feature)) return null;
    	
    	feature.setBBox(new Envelope());
    	
    	previousPbfOperationTimestamp = 0;
    	previousPbfOperationChangeset = 0;
    	previousPbfOperationRefId = 0l;
    	previousPbfOperationLongitude = 0;
    	previousPbfOperationLatitude = 0;
    	
    	for (PbfVgiOperation pbfOperation : pbfFeature.getOperationList()) {
	    	IVgiOperation operation = new VgiOperationImpl();
	    	
	    	/** geometry type */
	    	operation.setVgiGeometryType(feature.getVgiGeometryType());
	    	/** oid */
	    	operation.setOid(feature.getOid());
	    	
			/** op type */
			if (pbfOperation.hasVgiOperationType()) {
				operation.setVgiOperationType(VgiOperationType.getOperationTypeById(pbfOperation.getVgiOperationType()));
				previousOperationValues.setVgiOperationType(operation.getVgiOperationType());
			} else {
				operation.setVgiOperationType(previousOperationValues.getVgiOperationType());
			}
	    	if (!coordinateOnly) {
				/** user id */
				if (pbfOperation.hasUid()) {
					operation.setUid(pbfOperation.getUid());
					previousOperationValues.setUid(operation.getUid());
				} else {
					operation.setUid(previousOperationValues.getUid());
				}
	    	}
			/** timestamp */
			if (pbfOperation.hasTimestamp()) {
				operation.setTimestamp(new Date(((long)(pbfOperation.getTimestamp() + previousPbfOperationTimestamp + 1104537600)) * 1000));
				previousPbfOperationTimestamp += pbfOperation.getTimestamp();
				previousOperationValues.setTimestamp(operation.getTimestamp());
			} else {
				operation.setTimestamp(previousOperationValues.getTimestamp());
			}
			if (settings.getFilterTimestamp() != null && operation.getTimestamp().after(settings.getFilterTimestamp())) break;
		    if (!coordinateOnly) {
				/** changeset id */
				if (pbfOperation.hasChangesetId()) {
					operation.setChangesetid(pbfOperation.getChangesetId() + previousPbfOperationChangeset);
					previousPbfOperationChangeset += pbfOperation.getChangesetId();
					previousOperationValues.setChangesetid(operation.getChangesetid());
				} else {
					operation.setChangesetid(previousOperationValues.getChangesetid());
				}
				/** version */
				if (pbfOperation.hasVersion()) {
					operation.setVersion((short)pbfOperation.getVersion());
					previousOperationValues.setVersion(operation.getVersion());
				} else {
					operation.setVersion(previousOperationValues.getVersion());
				}
				/** ref id */
				if (pbfOperation.hasRef()) {
					operation.setRefId(pbfOperation.getRef() + previousPbfOperationRefId);
					previousPbfOperationRefId += pbfOperation.getRef();
				}
				/** key */
				if (pbfOperation.hasKey()) {
//					operation.setKey(pbfOperation.getKey().intern());
					operation.setKey(pbfOperation.getKey());
				}
				/** value */
				if (pbfOperation.hasValue()) {
//					if (pbfOperation.getKey().equals("highway") || pbfOperation.getKey().equals("building")) {
//						operation.setValue(pbfOperation.getValue().intern());
//					} else {
						operation.setValue(pbfOperation.getValue());
//					}
				}
				/** position */
				if (pbfOperation.hasPosition()) {
					operation.setPosition(pbfOperation.getPosition());
				}
	    	}
			/** coordinate */
			if (pbfOperation.hasLongitude()) {
				operation.setCoordinateFromInteger(pbfOperation.getLongitude() + previousPbfOperationLongitude, pbfOperation.getLatitude() + previousPbfOperationLatitude);
				previousPbfOperationLongitude += pbfOperation.getLongitude();
				previousPbfOperationLatitude += pbfOperation.getLatitude();
				feature.getBBox().expandToInclude(operation.getCoordinate());
			}
			
			feature.addToOperationList(operation);
    	}
		
		return feature;
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
