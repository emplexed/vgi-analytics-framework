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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.swing.event.EventListenerList;

import org.apache.logging.log4j.Logger;

import com.vividsolutions.jts.geom.Envelope;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiFeatureImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.FeatureImportListener;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.IQuadtree;

public class QuadtreeImpl implements IQuadtree {
	private static Logger log = org.apache.logging.log4j.LogManager.getLogger(QuadtreeImpl.class);
	
	private static EventListenerList listeners = new EventListenerList();
	
	private int level = 0;
//	private QuadtreeImpl parent = null;
	private QuadtreeImpl NW = null;
	private QuadtreeImpl NE = null;
	private QuadtreeImpl SE = null;
	private QuadtreeImpl SW = null;
	
	private double splitX = 0.0;
	private double splitY = 0.0;
	private double dimensionX = 180.0;
	private double dimensionY = 90.0;
	
	/** Quadtree path e.g. /NW/SE/SE/SW */
	private String path = "";
	
	/** feature are written to files, therefore not all features are in this featureList */
	private List<IVgiFeature> featureList = new ArrayList<IVgiFeature>();
	private int featureCount = 0;
	private final static int capacity = 100000;

	/** Constructor */
	public QuadtreeImpl() {	}
	
	public QuadtreeImpl(int level, String path) {
		this.level = level;
		this.path = path;
	}
	
	/**
	 * Inserts a feature to the quadtree
	 * @param feature quadtree feature (bounding box and list of operations)
	 */
	@Override
	public void insertFeature(IVgiFeature feature) {
		/** BBox is null if location of way/relation could not be found; reason: node cannot be found in input data (e.g. outside of country excerpt) */
		/** In this case, this feature is stored in the root */
		if (this.NW == null) {
			/** no sub-quadrants -> this is a leaf quadrant */
			//if (featureCount < capacity || level == 10 || feature.getBBox().isNull()) {
			if (featureCount < capacity || level == 10) {
				/** store in this quadrant if capacity is not reached OR quadrant has level 10, ... */
				featureList.add(feature);
				featureCount++;
			} else {
				/** ... otherwise subdivide */
				subdivideQuadrants();
				insertFeature(feature);
			}
			
		} else {
			/** Find suitable quadrant for current feature */
			if (feature.getBBox().getMaxX() < splitX && feature.getBBox().getMinY() > splitY) {
				this.NW.insertFeature(feature);
			} else if (feature.getBBox().getMinX() > splitX && feature.getBBox().getMinY() > splitY) {
				this.NE.insertFeature(feature);
			} else if (feature.getBBox().getMinX() > splitX && feature.getBBox().getMaxY() < splitY) {
				this.SE.insertFeature(feature);
			} else if (feature.getBBox().getMaxX() < splitX && feature.getBBox().getMaxY() < splitY) {
				this.SW.insertFeature(feature);
			} else {
				/** feature is too large for sub-quadrant, insert into this quadrant */
				featureList.add(feature);
				featureCount++;
			}
		}
	}
	
	private void subdivideQuadrants() {
		log.info("Subdivide Quadrant: {} (Level {})", path, level);
		/** create new quadrants in next level */
		this.NW = new QuadtreeImpl(level+1, path+"/NW");
		this.NE = new QuadtreeImpl(level+1, path+"/NE");
		this.SE = new QuadtreeImpl(level+1, path+"/SE");
		this.SW = new QuadtreeImpl(level+1, path+"/SW");
		
		/** calculate new quadrants' splits/dimensions */
		double newDimensionX = this.dimensionX * 0.5;
		double newDimensionY = this.dimensionY * 0.5;
		this.NW.setSplitX(this.splitX - newDimensionX);
		this.NE.setSplitX(this.splitX + newDimensionX);
		this.SE.setSplitX(this.splitX + newDimensionX);
		this.SW.setSplitX(this.splitX - newDimensionX);
		this.NW.setSplitY(this.splitY + newDimensionY);
		this.NE.setSplitY(this.splitY + newDimensionY);
		this.SE.setSplitY(this.splitY - newDimensionY);
		this.SW.setSplitY(this.splitY - newDimensionY);
		this.NW.setDimensionX(newDimensionX);
		this.NE.setDimensionX(newDimensionX);
		this.SE.setDimensionX(newDimensionX);
		this.SW.setDimensionX(newDimensionX);
		this.NW.setDimensionY(newDimensionY);
		this.NE.setDimensionY(newDimensionY);
		this.SE.setDimensionY(newDimensionY);
		this.SW.setDimensionY(newDimensionY);
//		this.NW.setParent(this);
//		this.NE.setParent(this);
//		this.SE.setParent(this);
//		this.SW.setParent(this);
		
		/** Load all features */
		if (featureList.size() < featureCount) {
			log.info(" - Import features before allocating them to sub quadrants");
			triggerFeatureImport(new FeatureImportEvent(this));
		} else {
			allocateFeatures();
		}
	}
	
	@Override
	public void allocateFeatures(List<IVgiFeature> featureList) {
		this.featureList.addAll(featureList);
		allocateFeatures();
	}
	
	private void allocateFeatures() {
		Collections.sort(featureList, VgiFeatureImpl.getFeatureComparator());
		
		List<IVgiFeature> featureToAllocate = new ArrayList<IVgiFeature>(featureList);
		
		/** Remove feature from this quadrant */
		featureList = new ArrayList<IVgiFeature>();
		this.featureCount = 0;
		
		/** Move features to sub-quadrants */
		for (IVgiFeature feature : featureToAllocate) {
			/** Features without BBox cannot be allocated */
//			if (feature.getBBox().isNull()) continue;
//			
//			Collections.sort(feature.getOperationList(), VgiOperationImpl.getVersionComparator()); //TODO is this necessary??
			insertFeature(feature);
		}
	}
	
	@Override
	public List<IVgiFeature> findFeature(Envelope bbox) {
//		List<IVgiFeature> featureList = null;
//		switch(geomType) {
//		case POINT:
//			featureList = nodeFeatureList;
//			break;
//		case LINE:
//			featureList = wayFeatureList;
//			break;
//		case RELATION:
//		default:
//			featureList = relationFeatureList;
//			break;
//		}
//		for (List<IVgiOperation> operations : featureList) {
//			for (IVgiOperation operation : operations) {
//				if (operation.getOid() == featureId) {
//					if (operation.getVgiGeometryType().equals(geomType)) return this;
//				} else if (operation.getOid() > featureId) {
//					break;
//				}
//			}
//		}
//		if (this.NW != null) {
//			PbfQuadtreeImpl p = this.NW.findFeature(geomType, featureId);
//			if (p != null) return p;
//			p = this.NE.findFeature(geomType, featureId);
//			if (p != null) return p;
//			p = this.SE.findFeature(geomType, featureId);
//			if (p != null) return p;
//			p = this.SW.findFeature(geomType, featureId);
//			if (p != null) return p;
//		}
//		return featureList;
		return null;
	}
	
	/** Add and remove feature import listener */
	public static void addFeatureImportListener(FeatureImportListener listener) {
		listeners.add(FeatureImportListener.class, listener);
	}
	public static void removeFeatureImportListener(FeatureImportListener listener) {
		listeners.remove(FeatureImportListener.class, listener);
	}
	/** trigger feature import */
	private synchronized void triggerFeatureImport(FeatureImportEvent event) {
		for (FeatureImportListener l : listeners.getListeners(FeatureImportListener.class))
			l.doImport(event);
	}
	
	@Override
	public List<IVgiFeature> getFeatureList() {
		return featureList;
	}
	@Override
	public void setFeatureList(List<IVgiFeature> featureList) {
		this.featureList = featureList;
	}

//	@Override
//	public QuadtreeImpl getParent() {
//		return parent;
//	}
//
//	@Override
//	public void setParent(QuadtreeImpl parent) {
//		this.parent = parent;
//	}

	@Override
	public void setNW(QuadtreeImpl nW) {
		NW = nW;
	}
	@Override
	public QuadtreeImpl getNW() {
		return NW;
	}

	@Override
	public void setNE(QuadtreeImpl nE) {
		NE = nE;
	}
	@Override
	public QuadtreeImpl getNE() {
		return NE;
	}

	@Override
	public void setSE(QuadtreeImpl sE) {
		SE = sE;
	}
	@Override
	public QuadtreeImpl getSE() {
		return SE;
	}

	@Override
	public void setSW(QuadtreeImpl sW) {
		SW = sW;
	}
	@Override
	public QuadtreeImpl getSW() {
		return SW;
	}
	
	@Override
	public String toString() {
		return "Quadtree (" + this.featureList.size() + "/" + this.featureCount + ")";
	}
	
	/** Getter/Setter */
	@Override
	public double getSplitX() {
		return splitX;
	}
	@Override
	public void setSplitX(double splitX) {
		this.splitX = splitX;
	}

	@Override
	public double getSplitY() {
		return splitY;
	}
	@Override
	public void setSplitY(double splitY) {
		this.splitY = splitY;
	}

	@Override
	public double getDimensionX() {
		return dimensionX;
	}
	@Override
	public void setDimensionX(double dimensionX) {
		this.dimensionX = dimensionX;
	}

	@Override
	public double getDimensionY() {
		return dimensionY;
	}
	@Override
	public void setDimensionY(double dimensionY) {
		this.dimensionY = dimensionY;
	}

	@Override
	public void setPath(String path) {
		this.path = path;
	}

	@Override
	public String getPath() {
		return path;
	}

	@Override
	public void setLevel(int level) {
		this.level = level;
	}
	@Override
	public int getLevel() {
		return level;
	}

	@Override
	public void setFeatureCount(int featureCount) {
		this.featureCount = featureCount;
	}
	@Override
	public int getFeatureCount() {
		return featureCount;
	}
}
