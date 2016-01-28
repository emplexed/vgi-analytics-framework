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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.geotools.geometry.jts.JTSFactoryFinder;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.IntersectionMatrix;
import com.vividsolutions.jts.geom.Polygon;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiFeatureImpl.LocalizeType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.QuadtreeIndexProto.PbfQuadtreeIndex;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.pbf.QuadtreeIndexProto.PbfQuadtreeIndex.PbfQuadtree;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

public class VgiOperationPbfReaderQuadtreeImpl extends VgiOperationPbfReaderImpl {

	private static Logger log = Logger.getLogger(VgiOperationPbfReaderQuadtreeImpl.class);
	
	private GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
	
	private int keepInCacheMaxLevel = 8;
	
	private CSVFileWriter writerQTStructure = new CSVFileWriter("E:/vgi/csv/quadtree_structure.csv");

	public VgiOperationPbfReaderQuadtreeImpl(IVgiPipelineSettings settings) {
		super(settings);
	}
	
	@Override
	public void run() {
		settings.setCache(new HashMap<String, List<IVgiFeature>>());
		
		checkRuntimeMemory();
		
		readPbfFiles();
	}
	
	public void checkRuntimeMemory() {
		Runtime runtime = Runtime.getRuntime();
		
//		log.info(runtime.freeMemory() + " < " + 1024*1024*1024 + "   " + runtime.maxMemory() + "    " + runtime.totalMemory() + "    " + (runtime.maxMemory() - runtime.totalMemory() + runtime.freeMemory()));
		
		if (runtime.maxMemory() - runtime.totalMemory() + runtime.freeMemory() < 1024*1024*512) { /** 512m */
		    
		    settings.getCache().clear();
		    settings.setCache(new HashMap<String, List<IVgiFeature>>());
		    settings.setKeepInCacheLevel(2);
			log.info(" - keepInCacheLevel=" + settings.getKeepInCacheLevel());
		} else {
			settings.setKeepInCacheLevel(settings.getKeepInCacheLevel()+1);
			if (settings.getKeepInCacheLevel() > keepInCacheMaxLevel) settings.setKeepInCacheLevel(keepInCacheMaxLevel);
			log.info(" - keepInCacheLevel=" + settings.getKeepInCacheLevel());
		}
	}
	
	protected void readPbfFiles() {
		/** Writer headings */
		writerQTStructure.writeLine("geometry;quadtree_path;level;feature_count;border_intersect;");
		
		/** Read the quadtree index */
		PbfQuadtreeIndex pbfQuadtree = null;
		try {

			if (!new File(settings.getPbfDataFolder() + "/Quadtree/index.pbf").exists()) {
				log.error("Cannot find file '" + settings.getPbfDataFolder() + "/Quadtree/index.pbf'");
				return;
			}
			
			pbfQuadtree = PbfQuadtreeIndex.parseFrom(new FileInputStream(settings.getPbfDataFolder() + "/Quadtree/index.pbf"));
			
			readPbfQuadtree(pbfQuadtree.getRoot());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		/** Close quadtree writer */
		writerQTStructure.closeFile();
	}
	
	private void readPbfQuadtree(PbfQuadtree pbfQuadtree) {		
		
		/** Build geometry of this quadrant */
		Coordinate[] coordinatesQuadrant = new Coordinate[5];
		coordinatesQuadrant[0] = new Coordinate(pbfQuadtree.getSplitx()-pbfQuadtree.getDimensionx(),pbfQuadtree.getSplity()-pbfQuadtree.getDimensiony());
		coordinatesQuadrant[1] = new Coordinate(pbfQuadtree.getSplitx()+pbfQuadtree.getDimensionx(),pbfQuadtree.getSplity()-pbfQuadtree.getDimensiony());
		coordinatesQuadrant[2] = new Coordinate(pbfQuadtree.getSplitx()+pbfQuadtree.getDimensionx(),pbfQuadtree.getSplity()+pbfQuadtree.getDimensiony());
		coordinatesQuadrant[3] = new Coordinate(pbfQuadtree.getSplitx()-pbfQuadtree.getDimensionx(),pbfQuadtree.getSplity()+pbfQuadtree.getDimensiony());
		coordinatesQuadrant[4] = coordinatesQuadrant[0];
		
		Polygon quadrant = geometryFactory.createPolygon(geometryFactory.createLinearRing(coordinatesQuadrant), null);
		quadrant.setSRID(4326);
//		Polygon quadrant = GeoHelper.createPolygon(coordinatesQuadrant, 4326);
		
		/** Read this quadtree if it intersects with test region and if it has >0 features */
		IntersectionMatrix intersectionMatrix = quadrant.relate(quadrant); /** default */
		if (settings.getFilterPolygon() != null) {
			intersectionMatrix = settings.getFilterPolygon().getPolygon().relate(quadrant); /** quadrant specific */
		}
		if (intersectionMatrix.isIntersects() && pbfQuadtree.getFeatureCount() > 0) {
			super.setPbfDataFolder(new File(settings.getPbfDataFolder() + "/Quadtree/" + pbfQuadtree.getPath()));
			
			if (intersectionMatrix.isWithin()) {
				super.setLocalizeType(LocalizeType.WITHIN);
			} else if (intersectionMatrix.isOverlaps(2, 2)) {
				super.setLocalizeType(LocalizeType.OVERLAPS);
			}
			
			/** Read or load features */
			if (pbfQuadtree.getLevel() <= settings.getKeepInCacheLevel()) {
				if (settings.getCache().containsKey(pbfQuadtree.getPath())) {
					/** Load features from cache */
					for (IVgiFeature feature : settings.getCache().get(pbfQuadtree.getPath())) {
			    		feature.setLocalizeType(localizeType);
			    		super.enqueueFeature(feature);
					}
				} else {
					super.cacheIdentifier = pbfQuadtree.getPath();
					super.readPbfFiles(true);
				}
			} else {
				super.readPbfFiles(false);
			}
			
			writerQTStructure.writeLine(quadrant.toText() + ";" + pbfQuadtree.getPath() + ";" + pbfQuadtree.getLevel() + ";" + pbfQuadtree.getFeatureCount() + ";" + intersectionMatrix.isOverlaps(2, 2) + ";");
		}
		
		if (pbfQuadtree.hasNw()) {
			readPbfQuadtree(pbfQuadtree.getNw());
			readPbfQuadtree(pbfQuadtree.getNe());
			readPbfQuadtree(pbfQuadtree.getSe());
			readPbfQuadtree(pbfQuadtree.getSw());
		}
    }
}
