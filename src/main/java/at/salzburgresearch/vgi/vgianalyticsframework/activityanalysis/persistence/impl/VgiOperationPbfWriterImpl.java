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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.IVgiOperationPbfWriter;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationFileListProto.PbfOperationFileList;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationFileListProto.PbfOperationFileList.ElementType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationFileListProto.PbfOperationFileList.PbfOperationFile;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationProto.PbfVgiOperationContainer;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationProto.PbfVgiOperationContainer.PbfVgiFeatureBatch;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationProto.PbfVgiOperationContainer.PbfVgiFeatureBytes;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationProto.PbfVgiOperationContainer.PbfVgiFeatureWrapper;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationProto.PbfVgiOperationContainer.PbfVgiOperation;

public class VgiOperationPbfWriterImpl implements IVgiOperationPbfWriter {
	private Logger log = Logger.getLogger(VgiOperationPbfWriterImpl.class);
	
	protected FileOutputStream pbfFeatureWriter = null;
//	protected FileOutputStream pbfStringWriter = null;
	
	protected PbfOperationFileList.Builder pbfFileListBuilder;
	protected PbfOperationFile.Builder pbfFileBuilder = null;
//	protected PbfStringList.Builder pbfStringListBuilder = null;
	
	protected File dataFolder = null;
	protected File pbfFile = null;
	
//	private List<StringEntry> stringList = null;
//	private int stringEntryIdMax = -1;
	
	private long previousPbfOperationOid = 0l;
	private int previousPbfOperationTimestamp = 0;
	private int previousPbfOperationChangesetId = 0;
	private long previousPbfOperationRefId = 0l;
	private int previousPbfOperationLongitude = 0;
	private int previousPbfOperationLatitude = 0;
	
//	protected int maxFileSize = 5;
	private int pbfFileOperationCount = 0;
	private static final int FEATURE_BATCH_SIZE = 100;
	private static final int NUM_OPERATIONS_PER_FILE = 500000;
	public static final int TIMESTAMP_OFFSET = 1104537600; // 1104537600 = 2005-01-01 00:00:00
	
	protected WriteMode writeMode = WriteMode.NOT_INITIALIZED;
	
	/** Stores previous operation attributes */
	protected IVgiOperation previousVgiOperationAttributes = new VgiOperationImpl();
	
	enum WriteMode {
		APPEND,
		SYNCHRONIZE,
		NOT_INITIALIZED
	}
	
	/** Constructor */
	public VgiOperationPbfWriterImpl() {
		
	}
	
	@Override
	public void initializePbfWriterToAppend(File dataFolder) {
		writeMode = WriteMode.APPEND;
		this.dataFolder = dataFolder;
		previousVgiOperationAttributes = new VgiOperationImpl();
		
		pbfFileListBuilder = PbfOperationFileList.newBuilder();
//		pbfStringListBuilder = PbfStringList.newBuilder();
//		stringList = new ArrayList<StringEntry>();
		
		try {
			if (new File(dataFolder + "/operationFileList.pbf").exists()) {
				/** Read existing data */
//				PbfOperationFileList pbfFileListOld = PbfOperationFileList.parseFrom(new FileInputStream(dataFolder + "/operationFileList.pbf"));
//				/** Add all previous files to file list */
//				for (int i=0; i<pbfFileListOld.getOperationFileCount(); i++) {
//					if (pbfFileListOld.getOperationFile(i).getElementType().equals(ElementType.NODE) || pbfFileListOld.getOperationFile(i).getOperationFileId() < 16721) {
//						pbfFileListBuilder.addOperationFile(pbfFileListOld.getOperationFile(i));
//					}
//				}
				pbfFileListBuilder = PbfOperationFileList.parseFrom(new FileInputStream(dataFolder + "/operationFileList.pbf")).toBuilder();
				
//				/** Select last file */
//				if (pbfFileListBuilder.getNodeOperationFileCount() > 0) {
//					pbfFileBuilder = pbfFileListBuilder.getNodeOperationFile(pbfFileListBuilder.getNodeOperationFileCount()-1).toBuilder();
//					pbfFileListBuilder.removeNodeOperationFile(pbfFileListBuilder.getNodeOperationFileCount()-1);
//				} else if (pbfFileListBuilder.getWayOperationFileCount() > 0) {
//					pbfFileBuilder = pbfFileListBuilder.getWayOperationFile(pbfFileListBuilder.getWayOperationFileCount()-1).toBuilder();
//					pbfFileListBuilder.removeWayOperationFile(pbfFileListBuilder.getWayOperationFileCount()-1);
//				} else if (pbfFileListBuilder.getRelationOperationFileCount() > 0) {
//					pbfFileBuilder = pbfFileListBuilder.getRelationOperationFile(pbfFileListBuilder.getRelationOperationFileCount()-1).toBuilder();
//					pbfFileListBuilder.removeRelationOperationFile(pbfFileListBuilder.getRelationOperationFileCount()-1);
//				} else {
//					log.error("Cannot open PBF file before writing features");
//					log.error(" - DataFolder: "  + dataFolder);
//				}
//				
//				/** Used to calculate oId difference */
//				previousPbfOperationOid = pbfFileBuilder.getMaxElementId();
//				
////				writePbfFileListTmp();
				
			} else {
				/** Create new file */
//				pbfFileBuilder = PbfOperationFile.newBuilder();
//				pbfFileBuilder.setOperationFileId(1);
//				pbfFileBuilder.setNumEntries(0);
//				pbfFileBuilder.setElementType(ElementType.NODE);
//				pbfFileBuilder.setMinElementId(Long.MAX_VALUE);
//				pbfFileBuilder.setMaxElementId(Long.MIN_VALUE);
//				
//				previousPbfOperationOid = 0;
			}
			
//			String elementTypePrefix = "d";
//			switch(pbfFileBuilder.getElementType()) {
//			case NODE:
//				elementTypePrefix = "n";
//				break;
//			case WAY:
//				elementTypePrefix = "w";
//				break;
//			case RELATION:
//				elementTypePrefix = "r";
//				break;
//			default:
//				break;
//			}
//			
//			pbfFile = new File(dataFolder + "/operation_" + elementTypePrefix + "_" + String.valueOf(pbfFileBuilder.getOperationFileId()) + ".pbf");
//			pbfFeatureListWriter = new FileOutputStream(pbfFile, true);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void initializePbfWriterToSynchronize(File dataFolder, VgiGeometryType geometryType, int fileId) {
		writeMode = WriteMode.SYNCHRONIZE;
		this.dataFolder = dataFolder;
		previousVgiOperationAttributes = new VgiOperationImpl();
		
		if (dataFolder == null) {
			log.error("PBF Writer has not been initialized!");
			return;
		}
		
		/** Read existing data */
		try {
			if (new File(dataFolder + "/operationFileList.pbf").exists()) {
				
//				PbfOperationFileList pbfFileListOld = PbfOperationFileList.parseFrom(new FileInputStream(dataFolder + "/operationFileList.pbf"));
//				pbfFileListBuilder = PbfOperationFileList.newBuilder();
//				/** Add all previous files to file list */
//				for (int i=0; i<pbfFileListOld.getNodeOperationFileCount(); i++) {
//					if (pbfFileListOld.getNodeOperationFile(i).getElementType().equals(ElementType.NODE)) {
//						pbfFileListBuilder.addNodeOperationFile(pbfFileListOld.getNodeOperationFile(i));
//					} else if (pbfFileListOld.getNodeOperationFile(i).getElementType().equals(ElementType.WAY)) {
//						pbfFileListBuilder.addWayOperationFile(pbfFileListOld.getNodeOperationFile(i));
//					} else if (pbfFileListOld.getNodeOperationFile(i).getElementType().equals(ElementType.RELATION)) {
//						pbfFileListBuilder.addRelationOperationFile(pbfFileListOld.getNodeOperationFile(i));
//					}
//				}
//				writePbfFileListTmp();
				
				/** Operations already exist in this directory */
				pbfFileListBuilder = PbfOperationFileList.parseFrom(new FileInputStream(dataFolder + "/operationFileList.pbf")).toBuilder();
				
				List<PbfOperationFile> fileList = null;
				String elementTypePrefix = "d";
				ElementType elementType = ElementType.UNDEFINED;
				
				if (geometryType.equals(VgiGeometryType.POINT)) {
					fileList = pbfFileListBuilder.getNodeOperationFileList();
					elementTypePrefix = "n";
					elementType = ElementType.NODE;
				} else if (geometryType.equals(VgiGeometryType.LINE)) {
					fileList = pbfFileListBuilder.getWayOperationFileList();
					elementTypePrefix = "w";
					elementType = ElementType.WAY;
				} else if (geometryType.equals(VgiGeometryType.RELATION)) {
					fileList = pbfFileListBuilder.getRelationOperationFileList();
					elementTypePrefix = "r";
					elementType = ElementType.RELATION;
				}
				
				pbfFileBuilder = null;
				
				for (PbfOperationFile file : fileList) {
					if (file.getOperationFileId() != fileId) continue;
					
					pbfFileBuilder = file.toBuilder();
					
					pbfFile = new File(dataFolder + "/operation_" + elementTypePrefix + "_" + String.valueOf(pbfFileBuilder.getOperationFileId()) + ".pbf");
					pbfFeatureWriter = new FileOutputStream(pbfFile, false);
					previousPbfOperationOid = pbfFileBuilder.getMaxElementId();
					
					break;
				}
				if (pbfFileBuilder == null) {
					/** Create new file */
					pbfFileBuilder = PbfOperationFile.newBuilder();
					pbfFileBuilder.setOperationFileId(fileList.size());
					pbfFileBuilder.setNumEntries(0);
					pbfFileBuilder.setElementType(elementType);
					pbfFileBuilder.setMinElementId(Long.MAX_VALUE);
					pbfFileBuilder.setMaxElementId(Long.MIN_VALUE);
					
					previousPbfOperationOid = 0;
					
					pbfFile = new File(dataFolder + "/operation_" + elementTypePrefix + "_" + String.valueOf(pbfFileBuilder.getOperationFileId()) + ".pbf");
					pbfFeatureWriter = new FileOutputStream(pbfFile, false);
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Closes the current operation PBF file and opens a old file or creates a new file for the specified OSM element type
	 * @param elementType node/way/relation
	 * @param createNewFile Create a new file or open an old file
	 */
	protected void openPbfDataFile(ElementType elementType, boolean createNewFile) {
		if (writeMode.equals(WriteMode.SYNCHRONIZE)) {
			log.error("should not happen");
		}

		if (writeMode.equals(WriteMode.APPEND)) {
				
			closePbfDataFile();
			
			String elementTypePrefix = "d";
			
			if (elementType.equals(ElementType.NODE)) {
				if (pbfFileListBuilder.getNodeOperationFileCount() == 0) createNewFile = true;
			} else if (elementType.equals(ElementType.WAY)) {
				if (pbfFileListBuilder.getWayOperationFileCount() == 0) createNewFile = true;
			} else if (elementType.equals(ElementType.RELATION)) {
				if (pbfFileListBuilder.getRelationOperationFileCount() == 0) createNewFile = true;
			}
			
			/** Create pbfFileBuilder */
			if (createNewFile) {
				pbfFileBuilder = PbfOperationFile.newBuilder();
				pbfFileBuilder.setNumEntries(0);
				pbfFileBuilder.setElementType(elementType);
				
				if (elementType.equals(ElementType.NODE)) {
					pbfFileBuilder.setOperationFileId(pbfFileListBuilder.getNodeOperationFileCount() + 1);
					elementTypePrefix = "n";
				} else if (elementType.equals(ElementType.WAY)) {
					pbfFileBuilder.setOperationFileId(pbfFileListBuilder.getWayOperationFileCount() + 1);
					elementTypePrefix = "w";
				} else if (elementType.equals(ElementType.RELATION)) {
					pbfFileBuilder.setOperationFileId(pbfFileListBuilder.getRelationOperationFileCount() + 1);
					elementTypePrefix = "r";
				}
//				log.info("open pbf file " + pbfFileBuilder.getOperationFileId());
				
				pbfFileBuilder.setMinElementId(Long.MAX_VALUE);
				pbfFileBuilder.setMaxElementId(Long.MIN_VALUE);
				
				previousPbfOperationOid = 0;
				previousPbfOperationTimestamp = 0;
				previousPbfOperationChangesetId = 0;
				previousPbfOperationRefId = 0l;
				previousPbfOperationLongitude = 0;
				previousPbfOperationLatitude = 0;
				
			} else {
				if (elementType.equals(ElementType.NODE)) {
					pbfFileBuilder = pbfFileListBuilder.getNodeOperationFile(pbfFileListBuilder.getNodeOperationFileCount()-1).toBuilder();
					pbfFileListBuilder.removeNodeOperationFile(pbfFileListBuilder.getNodeOperationFileCount()-1);
					elementTypePrefix = "n";
				} else if (elementType.equals(ElementType.WAY)) {
					pbfFileBuilder = pbfFileListBuilder.getWayOperationFile(pbfFileListBuilder.getWayOperationFileCount()-1).toBuilder();
					pbfFileListBuilder.removeWayOperationFile(pbfFileListBuilder.getWayOperationFileCount()-1);
					elementTypePrefix = "w";
				} else if (elementType.equals(ElementType.RELATION)) {
					pbfFileBuilder = pbfFileListBuilder.getRelationOperationFile(pbfFileListBuilder.getRelationOperationFileCount()-1).toBuilder();
					pbfFileListBuilder.removeRelationOperationFile(pbfFileListBuilder.getRelationOperationFileCount()-1);
					elementTypePrefix = "r";
				}

				previousPbfOperationOid = pbfFileBuilder.getMaxElementId();
				previousPbfOperationTimestamp = pbfFileBuilder.getLastTimestamp();
				previousPbfOperationChangesetId = pbfFileBuilder.getLastChangesetId();
				previousPbfOperationRefId = pbfFileBuilder.getLastRefId();
				previousPbfOperationLongitude = pbfFileBuilder.getLastLongitude();
				previousPbfOperationLatitude = pbfFileBuilder.getLastLatitude();
			}
			
			pbfFile = new File(dataFolder + "/operation_" + elementTypePrefix + "_" + String.valueOf(pbfFileBuilder.getOperationFileId()) + ".pbf");
//			File stringFile = new File(dataFolder + "/string_" + elementTypePrefix + "_" + String.valueOf((int)Math.floor((double)pbfFileBuilder.getOperationFileId() / 5)) + ".pbf");
			
			/** Prepare output stream */
			try {
				pbfFeatureWriter = new FileOutputStream(pbfFile, true);
//				pbfStringWriter = new FileOutputStream(stringFile, true);
//				pbfStringListBuilder = PbfStringList.parseFrom(new FileInputStream(stringFile)).toBuilder();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
//			} catch (IOException e) {
//				e.printStackTrace();
			}

//			for (PbfStringListEntry entry : pbfStringListBuilder.getEntryList()) {
//				stringList.add(new StringEntry(entry.getValue(), entry.getSearchId()));
//				if (entry.getSearchId() > stringEntryIdMax) stringEntryIdMax = entry.getSearchId();
//			}
			
			/** Reset previousOperationAttributes */
			previousVgiOperationAttributes = new VgiOperationImpl();
		}
		
//		/** If last PBF file is large enough, ... */
//		if (synchronizePbfFile == null) {
//			if (pbfFileListBuilder.getOperationFileCount() > 0) {
//				PbfOperationFile file = pbfFileListBuilder.getOperationFile(pbfFileListBuilder...)
//			}
//		}
//		if (pbfFile.exists() && synchronizePbfFile == null) {
//			dateinamen aus file list zusammenbauen
//			if (pbfFile.length() < minFileSize*1024*1024) {
//				/** ... add more operations to this PBF file and delete this file from the file list */
//				pbfFileListBuilder.removeOperationFile(pbfFileListBuilder.getOperationFileCount()-1);
//			}
//		}
//		
//		pbfFileBuilder.setOperationFileId(filenameIndex);
	}
	
	@Override
	public void terminatePbfWriter() {
		closePbfDataFile();
	}
	
	/**
	 * Closes the current operation PBF file
	 */
	protected void closePbfDataFile() {
		if (pbfFeatureWriter == null) return;
//		log.info("close pbf file " + pbfFileBuilder.getOperationFileId() + " " + pbfFileBuilder.getNumEntries());
		pbfFileBuilder.setLastTimestamp(previousPbfOperationTimestamp);
		pbfFileBuilder.setLastRefId(previousPbfOperationRefId);
		pbfFileBuilder.setLastChangesetId(previousPbfOperationChangesetId);
		pbfFileBuilder.setLastLongitude(previousPbfOperationLongitude);
		pbfFileBuilder.setLastLatitude(previousPbfOperationLatitude);
		pbfFileOperationCount = 0;
		
//		/** Write string file */
//		for (StringEntry entry : stringList) {
//			PbfStringListEntry.Builder entryBuilder = PbfStringListEntry.newBuilder();
//			entryBuilder.setValue(entry.getValue());
//			entryBuilder.setSearchId(entry.getSearchId());
//			pbfStringListBuilder.addEntry(entryBuilder.build());
//		}
//		stringList.clear();
//		try {
//			pbfStringListBuilder.build().writeTo(pbfStringWriter);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		/** Close file */
		try {
			pbfFeatureWriter.close();
//			pbfStringWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		/** Add PBF file to file list */
		if (writeMode.equals(WriteMode.APPEND)) {
			if (pbfFileBuilder.getNumEntries() == 0) {
				log.warn("pbfFileBuilder.getNumEntries() == 0 (can this happen?)");
				pbfFile.delete();
				pbfFile = null;
				pbfFeatureWriter = null;
				
			} else {
				if (pbfFileBuilder.getElementType().equals(ElementType.NODE)) {
					pbfFileListBuilder.addNodeOperationFile(pbfFileBuilder);
				} else if (pbfFileBuilder.getElementType().equals(ElementType.WAY)) {
					pbfFileListBuilder.addWayOperationFile(pbfFileBuilder);
				} else if (pbfFileBuilder.getElementType().equals(ElementType.RELATION)) {
					pbfFileListBuilder.addRelationOperationFile(pbfFileBuilder);
				}
			}
			
		} else if (writeMode.equals(WriteMode.SYNCHRONIZE)) {
			//TODO test if file is inserted correctly
			if (pbfFileBuilder.getElementType().equals(ElementType.NODE)) {
				if (pbfFileBuilder.getOperationFileId() <= pbfFileListBuilder.getNodeOperationFileCount()) {
					pbfFileListBuilder.setNodeOperationFile(pbfFileBuilder.getOperationFileId()-1, pbfFileBuilder);
				} else {
					pbfFileListBuilder.addNodeOperationFile(pbfFileBuilder);
					if (pbfFileBuilder.getOperationFileId() != pbfFileListBuilder.getNodeOperationFileCount()) log.error("");
				}
			} else if (pbfFileBuilder.getElementType().equals(ElementType.WAY)) {
				if (pbfFileBuilder.getOperationFileId() <= pbfFileListBuilder.getWayOperationFileCount()) {
					pbfFileListBuilder.setWayOperationFile(pbfFileBuilder.getOperationFileId()-1, pbfFileBuilder);
				} else {
					pbfFileListBuilder.addWayOperationFile(pbfFileBuilder);
				}
			} else if (pbfFileBuilder.getElementType().equals(ElementType.RELATION)) {
				if (pbfFileBuilder.getOperationFileId() <= pbfFileListBuilder.getRelationOperationFileCount()) {
					pbfFileListBuilder.setRelationOperationFile(pbfFileBuilder.getOperationFileId()-1, pbfFileBuilder);
				} else {
					pbfFileListBuilder.addRelationOperationFile(pbfFileBuilder);
				}
			}
			
//			String elementTypePrefix = "d";//TODO delete TMP-file???
//			switch(pbfFileBuilder.getElementType()) {
//			case NODE:
//				elementTypePrefix = "n";
//				break;
//			case WAY:
//				elementTypePrefix = "w";
//				break;
//			case RELATION:
//				elementTypePrefix = "r";
//				break;
//			default:
//				break;
//			}
//			
//			/** Delete old PBF file and rename new file */
//			File oldFile = new File(dataFolder + "/operation_" + elementTypePrefix + "_" + synchronizePbfFile.getOperationFileId() + ".pbf");
//	        if(oldFile.exists()){
//	        	oldFile.delete();
//	        	pbfFile.renameTo(new File(dataFolder + "/operation_" + elementTypePrefix + "_" + synchronizePbfFile.getOperationFileId() + ".pbf"));
//	        }
		}
		
		writePbfFileList();
		
		pbfFeatureWriter = null;
		pbfFile = null;
	}
	
	/**
	 * Writes the PBF file list
	 */
	protected void writePbfFileList() {
//		FileOutputStream pbfFileListWriter = null;
		try (FileOutputStream pbfFileListWriter = new FileOutputStream(new File(dataFolder + "/operationFileList.pbf"), false)) {
//			pbfFileListWriter = new FileOutputStream(new File(dataFolder + "/operationFileList.pbf"), false);
			
			pbfFileListBuilder.build().writeTo(pbfFileListWriter);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
//	/**
//	 * Writes the PBF file list
//	 */
//	protected void writePbfFileListTmp() {
//		FileOutputStream pbfFileListWriter = null;
//		try {
//			pbfFileListWriter = new FileOutputStream(new File(dataFolder + "/operationFileList_tmp.pbf"), false);
//			
//			pbfFileListBuilder.build().writeTo(pbfFileListWriter);
//			
//		} catch (IOException e) {
//			e.printStackTrace();
//		} finally {
//			IOUtils.closeQuietly(pbfFileListWriter);
//		}
//	}
	
	@Override
	public void writePbfFeature(IVgiFeature feature) {
		List<IVgiFeature> features = new ArrayList<IVgiFeature>();
		features.add(feature);
		writePbfFeatures(features);
	}
	
	@Override
	public void writePbfFeatures(List<IVgiFeature> featureBatch) {
		if (writeMode.equals(WriteMode.NOT_INITIALIZED)) { 
			log.error("PBF Writer has not been initialized!");
			return;
		}
		
		/** Open PBF feature file */
		if (pbfFeatureWriter == null) {
			if (featureBatch.get(0).getVgiGeometryType().equals(VgiGeometryType.POINT)) {
				openPbfDataFile(ElementType.NODE, false);
			} else if (featureBatch.get(0).getVgiGeometryType().equals(VgiGeometryType.LINE)) {
				openPbfDataFile(ElementType.WAY, false);
			} else if (featureBatch.get(0).getVgiGeometryType().equals(VgiGeometryType.RELATION)) {
				openPbfDataFile(ElementType.RELATION, false);
			}
		}
		
		List<PbfVgiOperation> pbfOperationBatch = new ArrayList<PbfVgiOperation>();
		
		for (IVgiFeature feature : featureBatch) {
			
			if (writeMode.equals(WriteMode.APPEND)) {
				/** Check if correct element id; if not, close current file and open new one */
				if (feature.getVgiGeometryType().equals(VgiGeometryType.POINT) && !pbfFileBuilder.getElementType().equals(ElementType.NODE)) {
//					write(pbfFeatureBatches);
					openPbfDataFile(ElementType.NODE, false);
				} else if (feature.getVgiGeometryType().equals(VgiGeometryType.LINE) && !pbfFileBuilder.getElementType().equals(ElementType.WAY)) {
//					write(pbfFeatureBatches);
					openPbfDataFile(ElementType.WAY, false);
				} else if (feature.getVgiGeometryType().equals(VgiGeometryType.RELATION) && !pbfFileBuilder.getElementType().equals(ElementType.RELATION)) {
//					write(pbfFeatureBatches);
					openPbfDataFile(ElementType.RELATION, false);
				}
				
				/** Create new file if current file size exceeds maximum file size */
				if (pbfFileOperationCount >= NUM_OPERATIONS_PER_FILE) {
//						if (pbfFile.length() > maxFileSize*1024*1024) {
//					log.info("operations: " + pbfFileOperationCount + " " + pbfFileBuilder.getOperationFileId());
					if (feature.getVgiGeometryType().equals(VgiGeometryType.POINT)) {
						handleOperationBatch(pbfOperationBatch);
						openPbfDataFile(ElementType.NODE, true);
					} else if (feature.getVgiGeometryType().equals(VgiGeometryType.LINE)) {
						handleOperationBatch(pbfOperationBatch);
						openPbfDataFile(ElementType.WAY, true);
					} else if (feature.getVgiGeometryType().equals(VgiGeometryType.RELATION)) {
						handleOperationBatch(pbfOperationBatch);
						openPbfDataFile(ElementType.RELATION, true);
					}
//					log.info("Create new pbf file" + " " + pbfFileBuilder.getOperationFileId());
				}
			}
			
			/** Calculate statistics for index file */
			if (feature.getOid() < pbfFileBuilder.getMinElementId()) pbfFileBuilder.setMinElementId(feature.getOid());
			if (feature.getOid() > pbfFileBuilder.getMaxElementId()) pbfFileBuilder.setMaxElementId(feature.getOid());
			
			/** Increase feature counter */
			pbfFileBuilder.setNumEntries(pbfFileBuilder.getNumEntries() + feature.getOperationList().size());
			pbfFileOperationCount += feature.getOperationList().size();
			
			/** build PBF feature */
			pbfOperationBatch.addAll(createPbfOperations(feature));
			
			if (pbfOperationBatch.size() >= FEATURE_BATCH_SIZE) {
				handleOperationBatch(pbfOperationBatch);
			}
		}
		
		/** Add PBF feature (wrapper) to PBF feature list */
		handleOperationBatch(pbfOperationBatch);
	}
	
	/**
	 * @param pbfOperationBatch
	 * @param feature
	 */
	private List<PbfVgiOperation> createPbfOperations(IVgiFeature feature) {
		List<PbfVgiOperation> pbfOperationBatch = new ArrayList<PbfVgiOperation>();
		Collections.sort(feature.getOperationList(), VgiOperationImpl.getVersionComparator());
		for (IVgiOperation operation : feature.getOperationList()) {
			PbfVgiOperation b = buildPbfOperation(operation).build();
			pbfOperationBatch.add(b);
		}
		return pbfOperationBatch;
	}
	
	/**
	 * @param pbfOperationBatch
	 */
	private void handleOperationBatch(List<PbfVgiOperation> pbfOperationBatch) {
		PbfVgiOperationContainer.Builder pbfFeatureBatches = PbfVgiOperationContainer.newBuilder();
		
		PbfVgiFeatureBatch.Builder pbfFeatureBatchBuilder = PbfVgiFeatureBatch.newBuilder();
		pbfFeatureBatchBuilder.addAllOperation(pbfOperationBatch);
		pbfOperationBatch.clear();
		
		/** build PBF feature wrapper (including number of bytes) */
		PbfVgiFeatureWrapper.Builder pbfFeatureWrapper = PbfVgiFeatureWrapper.newBuilder()
				.setBytes(PbfVgiFeatureBytes.newBuilder().setBytes(pbfFeatureBatchBuilder.build().toByteArray().length))
				.setFeature(pbfFeatureBatchBuilder);
		
		pbfFeatureBatches.addFeatureWrapper(pbfFeatureWrapper);
		write(pbfFeatureBatches);
		pbfFeatureBatches = PbfVgiOperationContainer.newBuilder();
	}
	
	private void write(PbfVgiOperationContainer.Builder pbfFeatureBatches) {
		/** Write the new operation file back to disk.*/
		try {
			pbfFeatureBatches.build().writeTo(pbfFeatureWriter);
			pbfFeatureBatches.clear();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private PbfVgiOperation.Builder buildPbfOperation(IVgiOperation operation) {
		PbfVgiOperation.Builder pbfOperation = PbfVgiOperation.newBuilder();
		
//		pbfOperation.setOid(operation.getOid() - previousPbfOperationOid);
//		previousPbfOperationOid = operation.getOid();

		/** id */
		if (operation.getOid() != previousPbfOperationOid) {
			pbfOperation.setOid(operation.getOid() - previousPbfOperationOid);
			previousPbfOperationOid = operation.getOid();
		}
		/** operation type */
		if (!operation.getVgiOperationType().equals(previousVgiOperationAttributes.getVgiOperationType())) {
			pbfOperation.setVgiOperationType(operation.getVgiOperationType().getId());
			previousVgiOperationAttributes.setVgiOperationType(operation.getVgiOperationType());
		}
		/** user id */
		if (operation.getUid() != previousVgiOperationAttributes.getUid()) {
			pbfOperation.setUid(operation.getUid());
			previousVgiOperationAttributes.setUid(operation.getUid());
		}
		/** time stamp */
		int timestamp = (int)(operation.getTimestamp().getTime() / 1000 - TIMESTAMP_OFFSET);
		if (timestamp != previousPbfOperationTimestamp) {
			pbfOperation.setTimestamp(timestamp - previousPbfOperationTimestamp);
			previousPbfOperationTimestamp = timestamp;
		}
		/** change set */
		if (operation.getChangesetid() != previousPbfOperationChangesetId) {
			pbfOperation.setChangesetId(operation.getChangesetid() - previousPbfOperationChangesetId);
			previousPbfOperationChangesetId = operation.getChangesetid();
		}
		/** version */
		if (operation.getVersion() != previousVgiOperationAttributes.getVersion()) {
			pbfOperation.setVersion(operation.getVersion());
			previousVgiOperationAttributes.setVersion(operation.getVersion());
		}
		/** reference Id */
		if (operation.getRefId() != -1) {
			pbfOperation.setRef(operation.getRefId() - previousPbfOperationRefId);
			previousPbfOperationRefId = operation.getRefId();
		}
		/** key */
		if (operation.getKey() != null && !operation.getKey().equals("")) {
			pbfOperation.setKey(operation.getKey());
		}
//		if (operation.getKey() != null && !operation.getKey().equals("")) { //TODO is "" allowed?
//			int i = addString(operation.getKey());
//			pbfOperation.setKey(i);
////			pbfOperation.setKey(addString(operation.getKey()));
//		}
		/** value */
		if (operation.getValue() != null && !operation.getValue().equals("")) {
			pbfOperation.setValue(operation.getValue());
		}
//		if (operation.getValue() != null && !operation.getValue().equals("")) {
//			int i = addString(operation.getValue());
//			pbfOperation.setValue(i);
////			pbfOperation.setValue(addString(operation.getValue()));
//		}
		/** position */
		if (operation.getPosition() != -1) {
			pbfOperation.setPosition(operation.getPosition());
		}
		/** point geometry */
		if (operation.getCoordinate() != null) {
			int[] coordinate = operation.getCoordinateAsInteger();
			pbfOperation.setLongitude(coordinate[0] - previousPbfOperationLongitude);
			pbfOperation.setLatitude(coordinate[1] - previousPbfOperationLatitude);
			
			previousPbfOperationLongitude = coordinate[0];
			previousPbfOperationLatitude = coordinate[1];
		}
		
		return pbfOperation;
	}
	
//	private int addString(String newString) {
////		for (int i=0; i<stringList.size(); i++) {
////			if (stringList.get(i).equals(newString)) {
////				return i;
////			}
////		}
////		stringList.add(newString);
////		return stringList.size()-1;
//		int index = Collections.binarySearch(this.stringList, new StringEntry(newString, -1), StringEntry.getStringComparator());
//		if (index < 0) {
//			stringEntryIdMax++;
//			stringList.add(index*(-1)-1, new StringEntry(newString, stringEntryIdMax));
//			return stringEntryIdMax;
//		} else {
//			return stringList.get(index).getSearchId();
//		}
//	}

	@Override
	public void setMaxFileSize(int maxFileSize) {
//		this.maxFileSize = maxFileSize;
	}
}
