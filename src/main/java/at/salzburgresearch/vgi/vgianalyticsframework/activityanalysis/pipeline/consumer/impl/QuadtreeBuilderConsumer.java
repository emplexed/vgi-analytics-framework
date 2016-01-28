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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.vividsolutions.jts.geom.Envelope;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.FeatureImportListener;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.IPbfQuadtree;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.IQuadtree;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.IVgiOperationPbfWriter;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.impl.FeatureImportEvent;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.impl.QuadtreeImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationFileListProto.PbfOperationFileList;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.OperationFileListProto.PbfOperationFileList.PbfOperationFile;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.QuadtreeIndexProto.PbfQuadtreeIndex;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.QuadtreeIndexProto.PbfQuadtreeIndex.PbfQuadtree;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipeline;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.IVgiPipelineConsumer;

public class QuadtreeBuilderConsumer implements IVgiPipelineConsumer, IPbfQuadtree, FeatureImportListener, ApplicationContextAware {
	private static Logger log = Logger.getLogger(QuadtreeBuilderConsumer.class);
	
	private ApplicationContext ctx;
	
	private IVgiPipelineSettings settings;
	
	private IQuadtree quadtree = null;
	
	/** Memory limit variables */
    private final static long usedMemoryLimit = 1024*1024*512*3; /** 1,5 GB */
    private int minNumFeaturesLimiter = 0;
    private long timeLastMemoryCheck = 0l;
    private int previousLimit = Integer.MAX_VALUE;
	
    /** statistics */
    private long addedToInsertListCount = 0l;
	private long writtenToFileCount = 0l;
	private long readFromFileCount = 0l;
	private long writtenQuadrants = 0l;
	
	/** Constructor */
	public QuadtreeBuilderConsumer() {
		QuadtreeImpl.addFeatureImportListener(this);
	}
	
	@Override
	public void doBeforeFirstBatch() {
		log.info("Import quadtree structure");
		importQuadtreeStructure();
	}
	
	@Override
	public void handleBatch(List<IVgiFeature> batch) {
		/** Add features to feature List */
		insertCollectedFeatures(batch);
		
		addedToInsertListCount += batch.size();
	}
	
	@Override
	public void doAfterLastBatch() {
		log.info("Do final tasks");
		/** Insert remaining features and write tree to files */
		insertCollectedFeatures(new ArrayList<IVgiFeature>());
		writePbfFile(0);
	}
	
	/** Import Quadtree Structure */
	@Override
	public void importQuadtreeStructure() {
		if (quadtree == null) {
			quadtree = new QuadtreeImpl();
			/** Read the quadtree index */
			PbfQuadtreeIndex pbfQuadtree = null;
			try {
				
				File indexFile = new File(settings.getPbfDataFolder() + "/Quadtree/index.pbf");
				if (!indexFile.exists()) {
					log.warn("Cannot find file '" + indexFile + "'! A new tree will be built.");
					return;
				}
				
				pbfQuadtree = PbfQuadtreeIndex.parseFrom(new FileInputStream(indexFile));
				
				readPbfQuadtree(pbfQuadtree.getRoot(), quadtree);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void readPbfQuadtree(PbfQuadtree pbfQuadtree, IQuadtree qt) {
		/** Read quadtree attributes */
		qt.setSplitX(pbfQuadtree.getSplitx());
		qt.setSplitY(pbfQuadtree.getSplity());
		qt.setDimensionX(pbfQuadtree.getDimensionx());
		qt.setDimensionY(pbfQuadtree.getDimensiony());
		qt.setPath(pbfQuadtree.getPath());
		qt.setLevel(pbfQuadtree.getLevel());
		qt.setFeatureCount(pbfQuadtree.getFeatureCount());
		
		if (pbfQuadtree.hasNw()) {
			qt.setNW(new QuadtreeImpl());
			readPbfQuadtree(pbfQuadtree.getNw(), qt.getNW());
			
			qt.setNE(new QuadtreeImpl());
			readPbfQuadtree(pbfQuadtree.getNe(), qt.getNE());
			
			qt.setSE(new QuadtreeImpl());
			readPbfQuadtree(pbfQuadtree.getSe(), qt.getSE());
			
			qt.setSW(new QuadtreeImpl());
			readPbfQuadtree(pbfQuadtree.getSw(), qt.getSW());
		}
    }
	
	private void determineBoundingBox(IVgiFeature feature) {
		feature.setBBox(new Envelope());
		
		for (IVgiOperation operation : feature.getOperationList()) {
			if (operation.getCoordinate() != null) {
				feature.getBBox().expandToInclude(operation.getCoordinate());
			}
		}
	}
	
	private void determineBoundingBox(List<IVgiFeature> featureList) {
		for (IVgiFeature feature : featureList) {
			determineBoundingBox(feature);
		}
	}
	
	@Override
	public void insertCollectedFeatures(List<IVgiFeature> featureList) {
		
		log.info("Insert " + featureList.size() + " features into QT");
	    
		/** If too much memory is used, write quadtree to files */
	    checkRuntimeMemory();
	    
	    /** Insert features into quadtree */
		int missing = 0;
		for (IVgiFeature feature : featureList) {
			
			determineBoundingBox(feature);
			
			if (!feature.getBBox().isNull()) {
				if (settings.getFilterPolygon() != null) {
					/** Only add features which intersect filter polygon */
					if (!feature.getBBox().intersects(settings.getFilterPolygon().getPolygon().getEnvelopeInternal())) continue;
				}
				/** Correctly processed feature */
				quadtree.insertFeature(feature);
				
			} else {
				missing++;
			}
		}
	    if (missing > 0) log.warn(" - " + missing + " features have no location");
		
		/** Clear feature lists */
		featureList = new ArrayList<IVgiFeature>();
		
		/** If too much memory is used, write quadtree to files */
	    checkRuntimeMemory();
	}
	
//	private void findFeatureLocation(List<IVgiFeature> features, boolean findAllLocations) {
////		int failedToLocalize = 0;
////		int failedToLocalizePrevious = 0;
////		
////		do {
////			failedToLocalizePrevious = failedToLocalize;
////			failedToLocalize = 0;
////			
////		    /** list which stores ways and relations. Later, their location will be determined */
////			List<IVgiFeature> parentFeatureList = new ArrayList<IVgiFeature>();
//////			List<Long> childNodeIds = new ArrayList<Long>();
////			TLongArrayList childNodeIds = new TLongArrayList();
////			List<Long> childWayIds = new ArrayList<Long>();
////			List<Long> childRelationIds = new ArrayList<Long>();
//			/** Process features */
//			for (IVgiFeature feature : features) {
//				
//				/** Initialize bbox */
//				if (feature.getBBox() == null) feature.setBBox(new Envelope());
//				/** If location of feature has already been found --> continue */
//				if (!feature.getBBox().isNull()) continue;
//				
//				if (feature.getLocalizeElementType().equals(VgiGeometryType.NONE)) continue;
//				
//				if (feature.getOperationList().get(0).getVgiGeometryType().equals(VgiGeometryType.POINT)) {
//					/** localizes itself */
//					feature.setLocalizeElementType(VgiGeometryType.NONE);
//					
//					/** NODES > read coordinate */
//					for (IVgiOperation operation : feature.getOperationList()) {
//						if (operation.getVgiOperationType().equals(VgiOperationType.OP_CREATE_NODE) || operation.getVgiOperationType().equals(VgiOperationType.OP_MODIFY_COORDINATE)) {
//							if (operation.getCoordinate() != null) {
////								try {
////									WKTReader wktReader = new WKTReader();
////									Geometry coordinate = wktReader.read(operation.getValue());
////									feature.getBBox().init(coordinate.getCentroid().getCoordinate());
////								} catch (ParseException e) {
////									e.printStackTrace();
////								}
//								feature.getBBox().init(operation.getCoordinate());
//							}
//						}
//					}
//					
////				} else if (feature.getOperationList().get(0).getVgiGeometryType().equals(VgiGeometryType.LINE)) {
////					/** LINES > add to parent feature list */
////					
////					if (feature.getLocalizeElementType().equals(VgiGeometryType.UNDEFINED)) findChildElementIds(feature);
////					
////					if (feature.getLocalizeElementId() != 0) {
////						parentFeatureList.add(feature);
////						
////						childNodeIds.add(feature.getLocalizeElementId());
////					} else {
////						/** ways without OP_ADD_NODE operations (due to license change) */
////					}
////					
////				} else if (feature.getOperationList().get(0).getVgiGeometryType().equals(VgiGeometryType.RELATION)) {
////					/** RELATIONS > add to parent feature list */
////					
////					if (feature.getLocalizeElementType().equals(VgiGeometryType.UNDEFINED)) findChildElementIds(feature);
////
////					if (feature.getLocalizeElementId() != 0) {
////						parentFeatureList.add(feature);
////						
////						if (feature.getLocalizeElementType().equals(VgiGeometryType.POINT)) {
////							childNodeIds.add(feature.getLocalizeElementId());
////						} else if (feature.getLocalizeElementType().equals(VgiGeometryType.LINE)) {
////							childWayIds.add(feature.getLocalizeElementId());
////						} else if (feature.getLocalizeElementType().equals(VgiGeometryType.RELATION)) {
////							childRelationIds.add(feature.getLocalizeElementId());
////						}
////					}
//				}
//			}
//			
////			/** Find location of line/relation features */
////			if (parentFeatureList.size() > 0) {
////				log.info("Find child feature location (" + parentFeatureList.size() + " features)");
////				
////				/** Set pipeline which finds the location of the feature */
////				
////				QuadtreeFeatureLocatorPipelineConsumer consumer = ctx.getBean("quadtreeFeatureLocatorPipelineConsumer", QuadtreeFeatureLocatorPipelineConsumer.class);
////				consumer.setFeatureList(parentFeatureList);
////
////				IVgiPipeline pipeline = ctx.getBean("quadtreeFeatureLocatorPipeline", IVgiPipeline.class);
////				pipeline.setFilterNodeId(childNodeIds);
////				pipeline.setFilterWayId(childWayIds);
////				pipeline.setFilterRelationId(childRelationIds);
////				pipeline.setConstrainedFilter(!findAllLocations);
////				pipeline.start();
////				
////				failedToLocalize = consumer.getNumFailedLocalizations();
////			}
////			
////			if (failedToLocalize>0 && failedToLocalize != failedToLocalizePrevious) {
////				log.info("Repeat findFeatureLocation (" + failedToLocalize + ")");
////			} else {
////				break;
////			}
////			
////		} while (true);
//	}
	
//	private void findChildElementIds(IVgiFeature feature) {
//		/** Relations in 2nd, 3rd, .. recursion loop already have a localization element */
//		if (!feature.getLocalizeElementType().equals(VgiGeometryType.UNDEFINED))  return;
//		
//		for (IVgiOperation operationFeature : feature.getOperationList()) {
//			if (operationFeature.getVgiOperationType().equals(VgiOperationType.OP_ADD_NODE)) {
//				feature.setLocalizeElementId(operationFeature.getRefId());
//				feature.setLocalizeElementType(VgiGeometryType.POINT);
//				return;
//			} else if (operationFeature.getVgiOperationType().equals(VgiOperationType.OP_ADD_MEMBER)) {
//				feature.setLocalizeElementId(operationFeature.getRefId());
//				if (operationFeature.getKey().equals("n")) {
//					feature.setLocalizeElementType(VgiGeometryType.POINT);
//				} else if (operationFeature.getKey().equals("w")) {
//					feature.setLocalizeElementType(VgiGeometryType.LINE);
//				} else if (operationFeature.getKey().equals("r")) {
//					feature.setLocalizeElementType(VgiGeometryType.RELATION);
//				}
//				return;
//			}
//		}
//		
//		
//		/** some feature do not have a localize element (e.g. way/0 ) */
//		if (feature.getLocalizeElementType().equals(VgiGeometryType.UNDEFINED)) {
//			feature.setLocalizeElementType(VgiGeometryType.NONE);
//		}
//	}
	
	@Override
	public void checkRuntimeMemory() {
		Runtime runtime = Runtime.getRuntime();
		/** TOTAL MEMORY - FREE MEMORY > USED MEMORY */
		long usedMemory = runtime.totalMemory() - runtime.freeMemory();
		
		if (usedMemory > usedMemoryLimit) {
		
		    /** If last memory check was performed less than 10 seconds ago, decrease minNumFeaturesLimiter */
		    if (new Date().getTime() - timeLastMemoryCheck < 1000*10) {
				minNumFeaturesLimiter++;
			    if (minNumFeaturesLimiter > 4) minNumFeaturesLimiter = 4;
		    } else {
		    	/** default values */
				minNumFeaturesLimiter = 1;
				previousLimit = Integer.MAX_VALUE;
			    timeLastMemoryCheck = new Date().getTime();
		    }
			
		    int limit = 0;
		    if (usedMemory > usedMemoryLimit + 1024 * 1024 * 1024) {
		    	limit = 0;
		    }  else if (usedMemory > usedMemoryLimit + 1024 * 1024 * 512) {
		    	limit = 36/minNumFeaturesLimiter;
		    }  else if (usedMemory > usedMemoryLimit + 1024 * 1024 * 256) {
		    	limit = 360/minNumFeaturesLimiter;
		    }  else if (usedMemory > usedMemoryLimit) {
		    	limit = 1800/minNumFeaturesLimiter;
		    }
		    
		    /** Only write file if current limit is lower than previous limit */
		    if (limit < previousLimit) {
		    	writePbfFile(limit);
		    	previousLimit = limit;
		    } else {
		    	
		    }
		}
	}
	
	/**
	 * Writes quadtree to PBF files
	 * @param minNumFeatures only quadrants with more than minNumFeatures features are written to files
	 */
	@Override
	public void writePbfFile(int minNumFeatures) {
		log.info("Write Quadrants with >" + minNumFeatures + " features to files");
		writtenQuadrants = 0l;
		
		/** Write tree */
		PbfQuadtreeIndex.Builder pbfQuadtreeIndex = PbfQuadtreeIndex.newBuilder();
		pbfQuadtreeIndex.setRoot(writeQuadrant(quadtree, new File(settings.getPbfDataFolder() + "/Quadtree/"), minNumFeatures));

		/** Write tree index file */
		String pbfFilePath = settings.getPbfDataFolder()+"/Quadtree/index.pbf";
		try {
			FileOutputStream pbfQuadtreeIndexWriter = new FileOutputStream(new File(pbfFilePath));
			
			pbfQuadtreeIndex.build().writeTo(pbfQuadtreeIndexWriter);
			
			if (pbfQuadtreeIndexWriter != null) pbfQuadtreeIndexWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		log.info(" - " + addedToInsertListCount + " features added + " + readFromFileCount + " features read = " + writtenToFileCount + " features written (" + writtenQuadrants + " quads affected)");
	}
	
	/** 
	 * Recursive method which writes features in quadrants to PBF files
	 * @param quadrant
	 * @param path
	 * @param minNumFeatures 
	 * @return PBF Quadtree Builder
	 */
	private PbfQuadtree.Builder writeQuadrant(IQuadtree quadrant, File path, int minNumFeatures) {
		if (!path.exists()) path.mkdir();
		
		/** Write this quad's operations to PBF (if enough and >0 feature exist) */
		if (quadrant.getFeatureList().size() >= minNumFeatures && quadrant.getFeatureList().size() > 0) {
			/** Load PBF writer */
			IVgiOperationPbfWriter writer = ctx.getBean("vgiOperationPbfWriter", IVgiOperationPbfWriter.class);
			writer.initializePbfWriterToAppend(path);
			
			/** Write features */
			for (IVgiFeature feature : quadrant.getFeatureList()) {
				writer.writePbfFeature(feature);
				feature = null;
			}
			
			writtenToFileCount += quadrant.getFeatureList().size();
			writtenQuadrants++;
			
			/** Delete features in quadtree (now they are only in PBF file) */
			quadrant.setFeatureList(new ArrayList<IVgiFeature>());
			/** Terminates PBF Writer (write index file, ... */
			writer.terminatePbfWriter();
		}
		
		PbfQuadtree.Builder pbfQuadtree = PbfQuadtree.newBuilder();
		/** write meta data */
		pbfQuadtree.setFeatureCount(quadrant.getFeatureCount());
		pbfQuadtree.setPath(quadrant.getPath());
		pbfQuadtree.setLevel(quadrant.getLevel());
		pbfQuadtree.setSplitx(quadrant.getSplitX());
		pbfQuadtree.setSplity(quadrant.getSplitY());
		pbfQuadtree.setDimensionx(quadrant.getDimensionX());
		pbfQuadtree.setDimensiony(quadrant.getDimensionY());
		
		/** write quarters */
		if (quadrant.getNW() != null) {
			pbfQuadtree.setNw(writeQuadrant(quadrant.getNW(), new File(path.getPath() + "/NW/"), minNumFeatures));
			pbfQuadtree.setNe(writeQuadrant(quadrant.getNE(), new File(path.getPath() + "/NE/"), minNumFeatures));
			pbfQuadtree.setSe(writeQuadrant(quadrant.getSE(), new File(path.getPath() + "/SE/"), minNumFeatures));
			pbfQuadtree.setSw(writeQuadrant(quadrant.getSW(), new File(path.getPath() + "/SW/"), minNumFeatures));
		}
		return pbfQuadtree;
	}
	
	/**
	 * Imports features from quadrant, deletes the PBF files and re-allocates them.
	 * @param e event 
	 */
	@Override
	public void doImport(FeatureImportEvent e) {
		
		/** Set pipeline which finds the features in this quad */
		IVgiPipeline pipeline = ctx.getBean("getQuadrantFeaturesPipeline", IVgiPipeline.class);
		pipeline.setPbfDataFolder(new File(settings.getPbfDataFolder() + "/Quadtree/" + ((QuadtreeImpl)e.getSource()).getPath()));
		pipeline.start();
		
		/** Read features in this quadrant */
		List<IVgiFeature> featureList = ctx.getBean("readAllFeaturesConsumer", ReadAllFeaturesConsumer.class).getFeatureList();
		
		/** Delete PBF files */
		deletePBFs(new File(settings.getPbfDataFolder() + "/Quadtree/" + ((QuadtreeImpl)e.getSource()).getPath()));
		
		readFromFileCount += featureList.size();
		
		determineBoundingBox(featureList);
		
		((QuadtreeImpl)e.getSource()).allocateFeatures(featureList);
	}
	
	/**
	 * Delete PBF files in specified directory
	 * @param path to directory which contains the PBF files
	 */
	private void deletePBFs(File path) {
		PbfOperationFileList pbfFileList = null;
		File fileList = new File(path + File.separator + "operationFileList.pbf");
		try {
			if (!fileList.exists()) {
				log.error("File '" + fileList.getAbsolutePath() + "' does not exist!");
				return;
			}
			
			FileInputStream fis = new FileInputStream(fileList);
			pbfFileList = PbfOperationFileList.parseFrom(fis);
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		deletePBF(pbfFileList.getNodeOperationFileList(), path);
		deletePBF(pbfFileList.getWayOperationFileList(), path);
		deletePBF(pbfFileList.getRelationOperationFileList(), path);
		
		if (fileList.renameTo(new File(fileList.getAbsolutePath()+"_"+System.currentTimeMillis()+".tmp"))) {
			log.warn("Cannot delete file " + fileList.getAbsolutePath());
		}
//		if (!fileList.delete()) { //TODO activate again and remove rename
//			log.warn("Cannot delete file " + fileList.getAbsolutePath());
////			fileList.deleteOnExit();
//		}
	}
	
	private void deletePBF(List<PbfOperationFile> pbfFileList, File path) {
		for (PbfOperationFile file : pbfFileList) {
			
			String elementTypePrefix = "d";
			switch (file.getElementType()) {
			case NODE:
				elementTypePrefix = "n";
				break;
			case WAY: 
				elementTypePrefix = "w";
				break;
			case RELATION: 
				elementTypePrefix = "r";
				break;
			default: break;
			}
			
			/** Delete it if it exists; else error. */
			File f = new File(path + "/operation_" + elementTypePrefix + "_" + file.getOperationFileId() + ".pbf");
			if (f.exists()) {
				if (!f.delete()) {
					log.warn("Cannot delete file " + f.getAbsolutePath());
				}
			} else {
				log.error("File '" + f.getAbsolutePath() + "' does not exist!");
			}
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext ctx) throws BeansException {
		this.ctx = ctx;
	}
	
	public void setSettings(IVgiPipelineSettings settings) {
		this.settings = settings;
	}
}
