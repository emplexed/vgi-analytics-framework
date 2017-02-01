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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeatureType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.IVgiPipelineConsumer;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.impl.VgiPipelineSettings;

public class FeatureBuilderConsumer implements IVgiPipelineConsumer {
	private static Logger log = Logger.getLogger(FeatureBuilderConsumer.class);
	
	private final SimpleDateFormat dateFormatOSM = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	
	private IVgiPipelineSettings settings = null;
	
	private GeometryFactory geomFactory = new GeometryFactory();
	
	private List<SimpleFeature> features = null;
	
	public FeatureBuilderConsumer() {
		features = new ArrayList<SimpleFeature>();
	}
	
	@Override
	public void doBeforeFirstBatch() { }
	
	@Override
	public void handleBatch(List<IVgiFeature> batch) {
		log.info("Start building features from operation");
		log.info(" - Cutoff date: " + dateFormatOSM.format(settings.getFilterTimestamp()) + ")");
		for (IVgiFeature feature : batch) {
			features.add(assembleGeometry(feature, null));
		}
	}
	
	public SimpleFeature assembleGeometry(IVgiFeature feature, IVgiFeatureType fixedFeatureType) {
		
		Geometry pointGeometry = null;
		Geometry pointGeometryDeleted = null;
		
		Map<String, String> attributes = new HashMap<String, String>();
		Map<String, String> attributesDeleted = null;
		
		List<Coordinate> coordinateArray = new ArrayList<Coordinate>();
		List<Coordinate> coordinateArrayDeleted = null;
		List<Long> nodeArray = new ArrayList<Long>();
		
		Collections.sort(feature.getOperationList(), VgiOperationImpl.getVersionComparator());
		
		for (IVgiOperation operation : feature.getOperationList()) {
			
			switch (operation.getVgiOperationType()) {
			
			case OP_CREATE_NODE:
			case OP_RECREATE_NODE:
				if (operation.getCoordinate() != null) {
					pointGeometry = geomFactory.createPoint(operation.getCoordinate());
				}
				pointGeometryDeleted = null;
				attributesDeleted = null;
				break;
				
			case OP_MODIFY_COORDINATE:
				if (operation.getCoordinate() != null) {
					pointGeometry = geomFactory.createPoint(operation.getCoordinate());
				}
				break;
				
			case OP_DELETE_NODE:
				pointGeometryDeleted = geomFactory.createGeometry(pointGeometry);
				attributesDeleted = new HashMap<String, String>(attributes); 
				pointGeometry = null;
				break;
				
			case OP_CREATE_WAY:
			case OP_RECREATE_WAY:
				coordinateArrayDeleted = null;
				attributesDeleted = null;
				break;
				
			case OP_ADD_NODE:
				if (coordinateArray.size() < operation.getPosition())
					log.warn("Index out of bounds... " + feature.getOid() + " " + operation.getRefId() + " " + operation.getTimestamp());
				
				coordinateArray.add(operation.getPosition(), (operation.getCoordinate() != null ? new Coordinate(operation.getCoordinate()) : null));
				nodeArray.add(operation.getPosition(), operation.getRefId());
				break;
				
			case OP_REMOVE_NODE:
				try {
					coordinateArray.remove(operation.getPosition());
					nodeArray.remove(operation.getPosition());
				} catch (IndexOutOfBoundsException e) {
					log.error(e);
				}
				break;
				
			case OP_REORDER_NODE:
				if (coordinateArray.size() < operation.getPosition()) {
					log.error("Index out of bounds... " + feature.getOid() + " " + operation.getRefId() + " " + operation.getTimestamp());
				}
				
				coordinateArray.add(operation.getPosition(), coordinateArray.remove((int)operation.getRefId()));
				nodeArray.add(operation.getPosition(), nodeArray.remove((int)operation.getRefId()));
				break;
				
			case OP_MODIFY_WAY_COORDINATE:
				for (int i=0; i<nodeArray.size(); i++) {
					if (nodeArray.get(i).equals(operation.getRefId())) {
						coordinateArray.set(i, operation.getCoordinate() != null ? new Coordinate(operation.getCoordinate()) : null);
					}
				}
				break;
				
			case OP_DELETE_WAY:
				coordinateArrayDeleted = new ArrayList<Coordinate>(coordinateArray);
				attributesDeleted = new HashMap<String, String>(attributes);
//				geometry = null;
				break;
				
			case OP_CREATE_RELATION:
//				osmFeature = factory.newRelation();
//				osmFeature.setId(operation.getOid());
//				osmFeature.setVisible(true);
				break;
				
			case OP_ADD_MEMBER:
//				if (operation.getKey().equals("n")) {
//					((IOsmRelation) osmFeature).addNodeMemberRef(operation.getRefId(), operation.getValue());
//				} else if  (operation.getKey().equals("w")) {
//					((IOsmRelation) osmFeature).addWayMemberRef(operation.getRefId(), operation.getValue());
//				} else if  (operation.getKey().equals("r")) {
//					((IOsmRelation) osmFeature).addRelationMemberRefs(operation.getRefId(), operation.getValue());
//				}
				break;
			case OP_REMOVE_MEMBER:
//				//TODO remove from member list (instead from map)
//				if (operation.getKey().equals("n")) {
//					((IOsmRelation) osmFeature).getNodeMemberRefs().remove(operation.getRefId());
//				} else if  (operation.getKey().equals("w")) {
//					((IOsmRelation) osmFeature).getWayMemberRefs().remove(operation.getRefId());
//				} else if  (operation.getKey().equals("r")) {
//					((IOsmRelation) osmFeature).getRelationMemberRefs().remove(operation.getRefId());
//				}
				break;
			case OP_REORDER_MEMBER:
				break;
			case OP_MODIFY_ROLE:
				//Relation OP_ADD_MEMBER
				// - Key: n/w/r
				// - Value: 45681307[outer]
				break;
			case OP_DELETE_RELATION:
				break;
			case OP_ADD_TAG:
				attributes.put(operation.getKey(), operation.getValue());
				operation.getValue().replace('\u200E', ' ');
				break;
			case OP_MODIFY_TAG_VALUE:
				attributes.put(operation.getKey(), operation.getValue());
				operation.getValue().replace('\u200E', ' ');
				break;
			case OP_REMOVE_TAG:
				attributes.remove(operation.getKey());
				break;
			default:
				break;
			}
		}

		boolean deleted = false;
		if (attributesDeleted != null) {
			pointGeometry = pointGeometryDeleted;
			coordinateArray = coordinateArrayDeleted;
			attributes = attributesDeleted;
			deleted = true;
		}
		
		if (coordinateArray != null) {
			List<Coordinate> toRemove = new ArrayList<Coordinate>();
			for (Coordinate c : coordinateArray) {
				if (c == null) toRemove.add(c); /** Don't create feature if not all coordinates are available (e.g. due to license change) */
			}
			coordinateArray.removeAll(toRemove);
		}
		
		/** Build geometry */
		boolean point = false;
		boolean lineString = false;
		boolean polygon = false;
		Coordinate[] coordinates = null;
		
//		if (pointGeometry instanceof Point) {
		if (pointGeometry != null) {
			/** geometry is ready */
			point = true;
			
		} else if (coordinateArray.size() >= 2) {
			coordinates = coordinateArray.toArray(new Coordinate[coordinateArray.size()]);
			boolean ring = (coordinates[0] != null && coordinates[0].equals(coordinates[coordinates.length-1]));
			if (ring) {
				if (coordinates.length >= 4) {
					lineString = true;
					polygon = true;
				}
			} else {
				lineString = true;
			}
		}
		
 		if (!point && !lineString && !polygon) return null;
		
		/** Find feature type */
 		SimpleFeatureType featureType = null;
 		if (fixedFeatureType != null) {
 			featureType = fixedFeatureType.getFeatureType();
 		} else {
 			featureType = findFeatureType(attributes, point, lineString, polygon).getFeatureType();
 		}
 		
		Geometry geometry = null;
 		
		/** Assemble feature */
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);
		for (AttributeDescriptor attribute : builder.getFeatureType().getAttributeDescriptors()) {
			if (attribute.getName().getLocalPart().equals("geom")) {
				/** Geometry */
				if (point && (featureType == null || featureType.getGeometryDescriptor().getType().getBinding().getSimpleName().equals("Point"))) {
					geometry = pointGeometry;
				} else if (polygon && (featureType == null || featureType.getGeometryDescriptor().getType().getBinding().getSimpleName().equals("Polygon"))) {
					geometry = geomFactory.createPolygon(geomFactory.createLinearRing(coordinates), null);
				} else if (lineString && (featureType == null || featureType.getGeometryDescriptor().getType().getBinding().getSimpleName().equals("LineString"))) {
					geometry = geomFactory.createLineString(coordinates);
				}
				builder.set("geom", geometry);
				
			} else if (attribute.getName().getLocalPart().equals("osm_id")) {
				/** OSM Id */
				builder.set("osm_id", feature.getOid());
				
			} else if (attribute.getName().getLocalPart().equals("deleted")) {
				/** Deleted? */
				builder.set("deleted", deleted);
				
			} else if (attributes.containsKey(attribute.getName().getLocalPart())) {
				/** Property */
				builder.set(attribute.getName().getLocalPart(), attributes.get(attribute.getName().getLocalPart()));
			}
		}
		if (geometry == null) return null;
		return builder.buildFeature(geometry.getGeometryType() + "_" + feature.getOid());
	}
	
	/**
	 * Finds feature type
	 * @param attributes
	 * @param point
	 * @param lineString
	 * @param polygon
	 * @return
	 */
	private IVgiFeatureType findFeatureType(Map<String, String> attributes, boolean point, boolean lineString, boolean polygon) {
		for (String featureTypeName : settings.getFeatureTypeList().keySet()) {
//			if (deleted) break;
			if (featureTypeName.equals(VgiPipelineSettings.invalidFeatureTypePoint)) continue;
			if (featureTypeName.equals(VgiPipelineSettings.invalidFeatureTypeLine)) continue;
			if (featureTypeName.equals(VgiPipelineSettings.invalidFeatureTypePolygon)) continue;
			
			Map<String, List<String>> tagsInclude = settings.getFeatureTypeList().get(featureTypeName).getFeatureTypeTagsInclude();
			Map<String, List<String>> tagsExclude = settings.getFeatureTypeList().get(featureTypeName).getFeatureTypeTagsExclude();
			
			for (String tagKey : tagsInclude.keySet()) {
				if (!attributes.containsKey(tagKey)) continue; /** wrong tag key */
				if (!tagsInclude.get(tagKey).isEmpty() && !tagsInclude.get(tagKey).contains(attributes.get(tagKey))) continue; /** wrong tag value */
				if (tagsExclude.containsKey(tagKey) && tagsExclude.get(tagKey).contains(attributes.get(tagKey))) continue; /** excluded tag */
				
				/** check geometry type */
				if (settings.getFeatureTypeList().get(featureTypeName).getFeatureType().getGeometryDescriptor().getType().getBinding().equals(Point.class) && !point) {
					continue;
				} else if (settings.getFeatureTypeList().get(featureTypeName).getFeatureType().getGeometryDescriptor().getType().getBinding().equals(LineString.class) && !lineString) {
					continue;
				} else if (settings.getFeatureTypeList().get(featureTypeName).getFeatureType().getGeometryDescriptor().getType().getBinding().equals(Polygon.class) && !polygon) {
					continue;
				}

				return settings.getFeatureTypeList().get(featureTypeName);
			}
		}
		
		if (point) {
			return settings.getFeatureTypeList().get(VgiPipelineSettings.invalidFeatureTypePoint);
		} else if (polygon) {
			return settings.getFeatureTypeList().get(VgiPipelineSettings.invalidFeatureTypePolygon);
		} else if (lineString) {
			return settings.getFeatureTypeList().get(VgiPipelineSettings.invalidFeatureTypeLine);
		}
		return null;
	}
	
	@Override
	public void doAfterLastBatch() {}
	
	public List<SimpleFeature> getFeatures() {
		return features;
	}
	
	public void setSettings(IVgiPipelineSettings settings) {
		this.settings = settings;
	}
}
