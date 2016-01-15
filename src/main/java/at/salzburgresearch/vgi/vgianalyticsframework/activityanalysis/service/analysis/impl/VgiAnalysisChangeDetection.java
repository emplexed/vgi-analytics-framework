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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.geotools.factory.Hints;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.IntersectionMatrix;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.operation.buffer.BufferParameters;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiActionImpl.ActionType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

/**
 * Analysis which detects changes between feature versions
 *
 */
public class VgiAnalysisChangeDetection extends VgiAnalysisParent implements IVgiAnalysisAction {
	
	private MathTransform transform = null;
	
	private long currentFeatureId = -1;
	
	private Map<SimpleFeatureType, List<EditingAction>> editingActionList = new ConcurrentHashMap<SimpleFeatureType, List<EditingAction>>();
	
	private Geometry previousGeometry = null;
	private SimpleFeatureType previousFeatureType = null;
	
	private String buildingType = null;
	private String buildingHousenumber = null;
	private String streetType = null;
	private String streetName = null;
	private String streetRef = null;
	private String streetMaxSpeed = null;
	
	private final int buildingBuffer = 20;
	private final int streetBuffer = 20;
	
	/** Constructor */
	public VgiAnalysisChangeDetection() {
		try {
			Hints hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
			CRSAuthorityFactory factory = ReferencingFactoryFinder.getCRSAuthorityFactory("EPSG", hints);
			CoordinateReferenceSystem sourceCRS = factory.createCoordinateReferenceSystem("EPSG:4326");
			CoordinateReferenceSystem targetCRS = factory.createCoordinateReferenceSystem("EPSG:32633");
	        boolean lenient = true; // allow for some error due to different datums
	        transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
		} catch (NoSuchAuthorityCodeException e) {
			e.printStackTrace();
		} catch (FactoryException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void analyze(IVgiAction action, Date timePeriod) {
		if (!editingActionList.containsKey(action.getFeatureType())) {
			editingActionList.put(action.getFeatureType(), new ArrayList<EditingAction>());
		}
		List<EditingAction> editingActionListFT = editingActionList.get(action.getFeatureType());
		
		/**
		 * For some OSM elements, an ID check is not enough. E.g. way/341048655 has highway (LineString) and building (Polygon) tag
		 */
		if (action.getOperations().get(0).getOid() != currentFeatureId || !action.getFeatureType().equals(previousFeatureType)) {
			previousGeometry = null;
			previousFeatureType = action.getFeatureType();
			currentFeatureId = action.getOperations().get(0).getOid();
			buildingType = null;
			buildingHousenumber = null;
			streetType = null;
			streetName = null;
			streetRef = null;
			streetMaxSpeed = null;
		}
		
		Geometry transformedGeometry = null;
        try {
        	transformedGeometry = JTS.transform((Geometry)(action.getFeature().getDefaultGeometry()), transform);
		} catch (TransformException e) {
		} catch (NullPointerException e) {
		}
        
        IVgiOperation op = action.getOperations().get(0);
        
        /**
         * Attribute checks
         */
		for (IVgiOperation operation : action.getOperations()) {
			
			if (operation.getVgiOperationType().equals(VgiOperationType.OP_ADD_TAG)) {
				if (action.getFeatureType().getName().getLocalPart().equals("building") || action.getFeatureType().getName().getLocalPart().equals("buildingPoint")) {
					if (operation.getKey().equals("building")) {
						buildingType = operation.getValue();
					} else if (operation.getKey().equals("addr:housenumber")) {
						buildingHousenumber = operation.getValue();
					}
				} else if (action.getFeatureType().getName().getLocalPart().equals("street")) {
					if (operation.getKey().equals("highway")) {
						streetType = operation.getValue();
					} else if (operation.getKey().equals("name")) {
						streetName = operation.getValue();
					} else if (operation.getKey().equals("ref")) {
						streetRef = operation.getValue();
					} else if (operation.getKey().equals("maxspeed")) {
						streetMaxSpeed = operation.getValue();
					}
				}
				
			} else if (operation.getVgiOperationType().equals(VgiOperationType.OP_MODIFY_TAG_VALUE)) {
				if (action.getFeatureType().getName().getLocalPart().equals("building") || action.getFeatureType().getName().getLocalPart().equals("buildingPoint")) {
					if (operation.getKey().equals("building")) {
						editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "BuildingTypeModified", buildingType, operation.getValue(), null, transformedGeometry, null));
						buildingType = operation.getValue();
					} else if (operation.getKey().equals("addr:housenumber")) {
						editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "BuildingHousenumberModified", buildingHousenumber, operation.getValue(), null, transformedGeometry, null));
						buildingHousenumber = operation.getValue();
					}
					
				} else if (action.getFeatureType().getName().getLocalPart().equals("street")) {
					if (operation.getKey().equals("highway")) {
						editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "StreetTypeModified", streetType, operation.getValue(), null, transformedGeometry, null));
						streetType = operation.getValue();
						
					} else if (operation.getKey().equals("name")) {
						//TODO if feature gets feature type 'street' later, streetName is null
						if (LevenshteinDistance(operation.getValue(), streetName) > 3) {
							editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "StreetNameModified", streetName, operation.getValue(), null, transformedGeometry, null));
						} else {
							editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "StreetNameModifiedLittle", streetName, operation.getValue(), null, transformedGeometry, null));
						}
						streetName = operation.getValue();
						
					} else if (operation.getKey().equals("ref")) {
						editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "StreetRefModified", streetRef, operation.getValue(), null, transformedGeometry, null));
						streetRef = operation.getValue();
						
					} else if (operation.getKey().equals("maxspeed")) {
						editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "StreetMaxSpeedModified", streetMaxSpeed, operation.getValue(), null, transformedGeometry, null));
						streetMaxSpeed = operation.getValue();
						
					}
				}
			}
		}

		/**
		 * Geometry changes
		 */
        
		double distanceNew = 0.0;
		double distanceOld = 0.0;
		double areaNew = 0.0;
		double areaOld = 0.0;
		
		if (previousGeometry != null && action.getFeature() != null) {
			
			if (action.getFeatureType().getName().getLocalPart().equals("building")) {
				areaOld = ((Polygon)previousGeometry).getArea();
				areaNew = ((Polygon)transformedGeometry).getArea();
				
		        double distance = transformedGeometry.distance(previousGeometry);
		        /** Geometry of e.g. way/10302049 has problems: nodes: ... <nd ref="86748755"/><nd ref="85997411"/><nd ref="85997415"/><nd ref="85997411"/><nd ref="85997415"/><nd ref="86749642"/> */
		        try {
		        	if (transformedGeometry.equals(previousGeometry)) {
		        		//
		        	} else if (transformedGeometry.intersects(previousGeometry)) {
						editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "BuildingIntersects", areaOld, areaNew, previousGeometry, transformedGeometry, null));
			        } else if (distance < buildingBuffer) { /**~ buffer */
						editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "BuildingWithinBuffer", null, distance, previousGeometry, transformedGeometry, null));
			        } else {
						editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "BuildingOutsideBuffer", null, distance, previousGeometry, transformedGeometry, null));
			        }
		        } catch (Exception e) {
		        	//
		        }
				
			} else if (action.getFeatureType().getName().getLocalPart().equals("buildingPoint")) {
				double distance = transformedGeometry.distance(previousGeometry);
				
				if (distance > buildingBuffer) {
					editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "BuildingOutsideBuffer", null, distance, previousGeometry, transformedGeometry, null));
				}
				
			} else if (action.getFeatureType().getName().getLocalPart().equals("street")) {
				IntersectionMatrix intersectionMatrixAB = transformedGeometry.relate(previousGeometry.buffer(streetBuffer, 3, BufferParameters.CAP_FLAT).buffer(1, 3, BufferParameters.CAP_ROUND));
				IntersectionMatrix intersectionMatrixBA = previousGeometry.relate(transformedGeometry.buffer(streetBuffer, 3, BufferParameters.CAP_FLAT).buffer(1, 3, BufferParameters.CAP_ROUND));
				
				distanceNew = ((LineString)transformedGeometry).getLength();
				distanceOld = ((LineString)previousGeometry).getLength();
				
				if (intersectionMatrixAB.isDisjoint()) {
					editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "StreetDisjoint", null, transformedGeometry.distance(previousGeometry), previousGeometry, transformedGeometry, null));
					
				} else if (intersectionMatrixAB.isWithin()) {
					if (distanceNew < distanceOld * 0.8) { /** new's distance is less than 80% of old street */
						/** Street shortened */
						Geometry diff = previousGeometry.difference(transformedGeometry.buffer(streetBuffer, 3, BufferParameters.CAP_FLAT));
						editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "StreetShortened", distanceOld, distanceNew, previousGeometry, transformedGeometry, diff));
					} else if (distanceNew != distanceOld) {
						editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "StreetWithinBuffer", distanceOld, distanceNew, null, transformedGeometry, null));
					}
					
				} else if (intersectionMatrixBA.isWithin()) {
					if (distanceNew > distanceOld * 1.2) { /** new's distance is greater than 120% of old street */
						/** Street shortened */
						Geometry diff = previousGeometry.difference(transformedGeometry.buffer(streetBuffer, 3, BufferParameters.CAP_FLAT));
						editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "StreetLengthened", distanceOld, distanceNew, previousGeometry, transformedGeometry, diff));
					} else if (distanceNew != distanceOld) {
						editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "StreetWithinBuffer", distanceOld, distanceNew, null, transformedGeometry, null));
					}
					
				} else {
					editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "StreetCrossesBuffer", distanceOld, distanceNew, previousGeometry, transformedGeometry, null));
				}
			}
		} else if (action.getFeature() != null) {
			if (action.getFeatureType().getName().getLocalPart().equals("street")) {
				distanceNew = ((LineString)transformedGeometry).getLength();
			}
		}
		
		/**
		 * Feature checks (create/delete/merge/split)
		 */
		for (IVgiOperation operation : action.getOperations()) {
			if (operation.getVgiOperationType().equals(VgiOperationType.OP_SPLIT_WAY)) {
				editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "GeometrySplit", null, distanceNew, previousGeometry, transformedGeometry, null));
				break;
			} else if (operation.getVgiOperationType().equals(VgiOperationType.OP_MERGE_WAY)) {
				editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "GeometryMerge", distanceOld, null, previousGeometry, transformedGeometry, null));
				break;
			} 
		}
		
		if (action.getActionType().equals(ActionType.CREATE)) {
			if (action.getFeature() != null) {
				if (!op.getVgiOperationType().equals(VgiOperationType.OP_SPLIT_WAY)) {
					editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "CreateGeometry", null, distanceNew, null, transformedGeometry, null));
				}
			}
		}
		if (action.getActionType().equals(ActionType.DELETE)) {
			if (action.getFeature() != null) {
				if (!op.getVgiOperationType().equals(VgiOperationType.OP_MERGE_WAY)) {
					editingActionListFT.add(new EditingAction(op.getVgiGeometryType(), op.getOid(), op.getVersion(), op.getChangesetid(), op.getTimestamp(), "DeleteGeometry", distanceOld, null, null, transformedGeometry, null));
				}
			}
		}
		
		previousGeometry = transformedGeometry;
	}
	
	@Override
	public void write(File path) {
		/** iterate through users*/
		for (SimpleFeatureType ft : editingActionList.keySet()) {
			
			CSVFileWriter writer = new CSVFileWriter(path + "/change_detection_" + ft.getName().getLocalPart() + "_geometry.csv");
			/** write header */
			writer.writeLine("changeId;elementType;osmId;version;changesest;timestamp;status;valueOld;valueNew;geometryOld;geometryNew;geometryDiff");
			
			int i = 1;
			
			for (EditingAction e : editingActionList.get(ft)) {
				writer.writeLine(i++ + ";" +
						e.elementType + ";" + 
						e.osmId + ";" + 
						e.version + ";" + 
						e.changeset + ";" + 
						dateTimeFormat.format(e.timestamp) + ";" + 
						e.status + ";" + 
						(e.valueOld != null ? (e.valueOld instanceof Double ? decimalFormat.format(e.valueOld) : "\""+e.valueOld.toString()+"\"") : "") + ";" + 
						(e.valueNew != null ? (e.valueNew instanceof Double ? decimalFormat.format(e.valueNew) : "\""+e.valueNew.toString()+"\"") : "") + ";" + 
						(e.geometryOld != null ? e.geometryOld.toText() : "") + ";" + 
						(e.geometryNew != null ? e.geometryNew.toText() : "") + ";" + 
						(e.geometryDiff != null ? e.geometryDiff.toText() : ""));
			}
			
			writer.closeFile();
			
			/** NO GEOM */
			
			writer = new CSVFileWriter(path + "/change_detection_" + ft.getName().getLocalPart() + ".csv");
			/** write header */
			writer.writeLine("changeId;elementType;osmId;version;changesest;timestamp;status;valueOld;valueNew;geometryOld;geometryNew;geometryDiff");
			
			i = 1;
			
			for (EditingAction e : editingActionList.get(ft)) {
				writer.writeLine(i++ + ";" +
						e.elementType + ";" + 
						e.osmId + ";" + 
						e.version + ";" + 
						e.changeset + ";" + 
						dateTimeFormat.format(e.timestamp) + ";" + 
						e.status + ";" + 
						(e.valueOld != null ? (e.valueOld instanceof Double ? decimalFormat.format(e.valueOld) : "\""+e.valueOld.toString()+"\"") : "") + ";" + 
						(e.valueNew != null ? (e.valueNew instanceof Double ? decimalFormat.format(e.valueNew) : "\""+e.valueNew.toString()+"\"") : ""));
			}
			
			writer.closeFile();
		}
	}
	
	@Override
	public void reset() {
		editingActionList.clear();
	}
	
	@Override
	public String toString() {
		return "VgiAnalysisChangeDetection";
	}
	
	/**
	 * Source: http://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/
	 * Levenshtein_distance#Java
	 */
	public int LevenshteinDistance(String s0, String s1) {
		if (s1 == null) {
			s1 = "";
		}

		int len0 = s0.length() + 1;
		int len1 = s1.length() + 1;

		// the array of distances
		int[] cost = new int[len0];
		int[] newcost = new int[len0];

		// initial cost of skipping prefix in String s0
		for (int i = 0; i < len0; i++)
			cost[i] = i;

		// dynamicaly computing the array of distances

		// transformation cost for each letter in s1
		for (int j = 1; j < len1; j++) {
			// initial cost of skipping prefix in String s1
			newcost[0] = j;

			// transformation cost for each letter in s0
			for (int i = 1; i < len0; i++) {
				// matching current letters in both strings
				int match = (s0.charAt(i - 1) == s1.charAt(j - 1)) ? 0 : 1;

				// computing cost for each transformation
				int cost_replace = cost[i - 1] + match;
				int cost_insert = cost[i] + 1;
				int cost_delete = newcost[i - 1] + 1;

				// keep minimum cost
				newcost[i] = Math.min(Math.min(cost_insert, cost_delete), cost_replace);
			}

			// swap cost/newcost arrays
			int[] swap = cost;
			cost = newcost;
			newcost = swap;
		}

		// the distance is the cost for transforming all letters in both strings
		return cost[len0 - 1];
	}
	
	private class EditingAction {
		private VgiGeometryType elementType;
		private long osmId;
		private short version;
		private int changeset;
		private Date timestamp;
		private String status;
		private Object valueOld;
		private Object valueNew;
		private Geometry geometryOld;
		private Geometry geometryNew;
		private Geometry geometryDiff;
		public EditingAction(VgiGeometryType elementType,
				long osmId, short version, int changeset, Date timestamp, String status, Object valueOld,
				Object valueNew, Geometry geometryOld, Geometry geometryNew, Geometry geometryDiff) {
			this.elementType = elementType;
			this.osmId = osmId;
			this.version = version;
			this.changeset = changeset;
			this.timestamp = timestamp;
			this.status = status;
			this.valueOld = valueOld;
			this.valueNew = valueNew;
			this.geometryOld = geometryOld;
			this.geometryNew = geometryNew;
			this.geometryDiff = geometryDiff;
		}
	}
}
