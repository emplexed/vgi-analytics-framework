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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeatureType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiActionDefinitionRule;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiActionImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiFeatureTypeImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiPolygon;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisActionDetails;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisActionPerFeatureType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisActionPerType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisBatchContributor;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisBatchGeneral;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisBatchUserActionType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisChangeDetection;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisFeatureStability;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisHourOfDay;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisOperationPerType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisTags;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisUpdate;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisUpdateGeometry;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisUserPerAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisUserPerOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisUserPerTags;

/**
 * Class which loads from a XML file and manages VGI settings.
 *
 */
public class VgiPipelineSettings implements IVgiPipelineSettings {
	private static Logger log = Logger.getLogger(VgiPipelineSettings.class);
	
	private SimpleDateFormat dateFormatOSM = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	
	private boolean settingsLoaded = false;
	
	private String settingName = "Default";
	
	private File pbfDataFolder = null;
	private boolean readQuadtree = false;
	
	/** Filter */
	private long filterUid = -1;
	private Date filterTimestamp = null;
	private VgiGeometryType filterElementType = VgiGeometryType.UNDEFINED;
	private Map<String, List<String>> filterTag = null;
	private List<VgiPolygon> filterPolygonList = null;
	private VgiPolygon filterPolygon = null;
//	private String filterPolygonLabel = "Default";
	
	/** Action generator */
	private long actionTimeBuffer = 43200000l;
	private List<IVgiAction> actionDefinitionList = null;
	private Map<String, IVgiFeatureType> featureTypeList = null;
	public static final String invalidFeatureTypePoint = "[invalidPoint]";
	public static final String invalidFeatureTypeLine = "[invalidLine]";
	public static final String invalidFeatureTypePolygon = "[invalidPolygon]";
	
	/** Analysis */
	private File resultFolder = null;
	private Date analysisStartDate = null;
	private Date analysisEndDate = null;
	private boolean ignoreFeaturesWithoutTags = false;
	private boolean findRelatedOperations = false;
	private boolean writeGeometryFiles = false;
	private String temporalResolution = "year";
	private boolean ignoreNonFeatureTypeTags = false;
	
	private List<IVgiAnalysisAction> actionAnalyzerList = null;
	private List<IVgiAnalysisOperation> operationAnalyzerList = null;
	private List<IVgiAnalysisFeature> featureAnalyzerList = null;

	private Map<String, List<IVgiFeature>> cache = null;
	private int keepInCacheLevel = 2;
	
	
	public VgiPipelineSettings() {
		dateFormatOSM.setTimeZone(TimeZone.getTimeZone("UTC"));

		featureTypeList = new HashMap<String, IVgiFeatureType>();
		
		IVgiFeatureType vgiFeatureTypePoint = new VgiFeatureTypeImpl();
		SimpleFeatureTypeBuilder featureTypePoint = new SimpleFeatureTypeBuilder();
		featureTypePoint.setName(invalidFeatureTypePoint);
		featureTypePoint.setCRS(DefaultGeographicCRS.WGS84);
		featureTypePoint.add("geom", Point.class);
		featureTypePoint.setDefaultGeometry("geom");
		featureTypePoint.add("osm_id", Long.class);
		featureTypePoint.add("deleted", Boolean.class);
		vgiFeatureTypePoint.setFeatureType(featureTypePoint.buildFeatureType());
		vgiFeatureTypePoint.getFeatureTypeTags().put("invalid", new ArrayList<String>());
		featureTypeList.put(featureTypePoint.getName(), vgiFeatureTypePoint);
		
		IVgiFeatureType vgiFeatureTypeLine = new VgiFeatureTypeImpl();
		SimpleFeatureTypeBuilder featureTypeLine = new SimpleFeatureTypeBuilder();
		featureTypeLine.setName(invalidFeatureTypeLine);
		featureTypeLine.setCRS(DefaultGeographicCRS.WGS84);
		featureTypeLine.add("geom", LineString.class);
		featureTypeLine.setDefaultGeometry("geom");
		featureTypeLine.add("osm_id", Long.class);
		featureTypeLine.add("deleted", Boolean.class);
		vgiFeatureTypeLine.setFeatureType(featureTypeLine.buildFeatureType());
		vgiFeatureTypeLine.getFeatureTypeTags().put("invalid", new ArrayList<String>());
		featureTypeList.put(featureTypeLine.getName(), vgiFeatureTypeLine);
		
		IVgiFeatureType vgiFeatureTypePolygon = new VgiFeatureTypeImpl();
		SimpleFeatureTypeBuilder featureTypePolygon = new SimpleFeatureTypeBuilder();
		featureTypePolygon.setName(invalidFeatureTypePolygon);
		featureTypePolygon.setCRS(DefaultGeographicCRS.WGS84);
		featureTypePolygon.add("geom", Polygon.class);
		featureTypePolygon.setDefaultGeometry("geom");
		featureTypePolygon.add("osm_id", Long.class);
		featureTypePolygon.add("deleted", Boolean.class);
		vgiFeatureTypePolygon.setFeatureType(featureTypePolygon.buildFeatureType());
		vgiFeatureTypePolygon.getFeatureTypeTags().put("invalid", new ArrayList<String>());
		featureTypeList.put(featureTypePolygon.getName(), vgiFeatureTypePolygon);
	}
	
	@Override
	public void loadSettings(File url) {
		if (settingsLoaded) {
			log.info("Settings already loaded!");
			return;
		}
		if (url == null) {
			log.info("No settings file specified");
			return;
		}
		
	    try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse (url);
            
            doc.getDocumentElement().normalize();
            
            NodeList nodeList = doc.getElementsByTagName("general");
            
            for(int s=0; s<nodeList.getLength(); s++) {
                Node node = nodeList.item(s);
				if (node.getNodeType() != Node.ELEMENT_NODE) continue; 
					
				Element firstElement = (Element) node;

				/** settingName */
				settingName = firstElement.getAttribute("settingName");
				
				/** pbfDataFolder */
				pbfDataFolder = new File(firstElement.getAttribute("pbfDataFolder"));
				if (!pbfDataFolder.exists()) {
					log.error("Cannot find pbfDataFolder!");
					System.exit(1);
				}
				
				/** resultFolder */
    			if (firstElement.hasAttribute("resultFolder")) {
					resultFolder = new File(firstElement.getAttribute("resultFolder"));
					if (!resultFolder.exists()) {
						log.error("Cannot find resultFolder!");
						System.exit(1);
					}
					SimpleDateFormat dateFormatOSM = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss'Z'");
					resultFolder = new File(resultFolder + File.separator + dateFormatOSM.format(new Date()) + File.separator);
					resultFolder.mkdir();
    			}
    			
				/** readQuadtree */
    			if (firstElement.hasAttribute("readQuadtree")) {
	    			try {
	    				readQuadtree = Boolean.parseBoolean(firstElement.getAttribute("readQuadtree"));
	    			} catch (Exception ex) {
	    				log.warn("Cannot parse setting 'readQuadtree' (Boolean)");
	    			}
    			}
    			
				/** filterTimestamp */
    			if (firstElement.hasAttribute("filterTimestamp")) {
	    			try {
	    				filterTimestamp = dateFormatOSM.parse(firstElement.getAttribute("filterTimestamp"));
	    			} catch (java.text.ParseException ex) {
	    				log.warn("Cannot parse setting 'filterTimestamp' (Long)");
	    			}
    			}
            }
            
            nodeList = doc.getElementsByTagName("analysis");
            
            for(int s=0; s<nodeList.getLength(); s++) {
                Node node = nodeList.item(s);
				if (node.getNodeType() != Node.ELEMENT_NODE) continue; 
				
				Element firstElement = (Element) node;
				
				/** findRelatedOperations */
    			if (firstElement.hasAttribute("findRelatedOperations")) {
	    			try {
	    				findRelatedOperations = Boolean.parseBoolean(firstElement.getAttribute("findRelatedOperations"));
	    			} catch (Exception ex) {
	    				log.warn("Cannot parse setting 'findRelatedOperations' (Boolean)");
	    			}
    			}
    			
				/** writeGeometryFiles */
    			if (firstElement.hasAttribute("writeGeometryFiles")) {
	    			try {
	    				writeGeometryFiles = Boolean.parseBoolean(firstElement.getAttribute("writeGeometryFiles"));
	    			} catch (Exception ex) {
	    				log.warn("Cannot parse setting 'writeGeometryFiles' (Boolean)");
	    			}
    			}
    			
				/** analysisStartDate */
    			if (firstElement.hasAttribute("analysisStartDate")) {
	    			try {
	    				analysisStartDate = dateFormatOSM.parse(firstElement.getAttribute("analysisStartDate"));
	    			} catch (java.text.ParseException ex) {
	    				log.warn("Cannot parse setting 'analysisStartDate' (DateTime yyyy-MM-dd'T'HH:mm:ss'Z')");
	    			}
    			}
    			
				/** analysisEndDate */
    			if (firstElement.hasAttribute("analysisEndDate")) {
	    			try {
	    				analysisEndDate = dateFormatOSM.parse(firstElement.getAttribute("analysisEndDate"));
	    			} catch (java.text.ParseException ex) {
	    				log.warn("Cannot parse setting 'analysisEndDate' (DateTime yyyy-MM-dd'T'HH:mm:ss'Z')");
	    			}
    			}
    			
				/** temporalResolution */
    			if (firstElement.hasAttribute("temporalResolution")) {
	    			try {
	    				temporalResolution = firstElement.getAttribute("temporalResolution");
	    			} catch (Exception ex) {
	    				log.warn("Cannot parse setting 'temporalResolution' (data type: String)");
	    			}
    			}
    			
				/** filterElementType */
    			if (firstElement.hasAttribute("filterElementType")) {
	    			try {
	    				filterElementType = VgiGeometryType.valueOf(firstElement.getAttribute("filterElementType"));
	    			} catch (Exception ex) {
	    				log.warn("Cannot parse setting 'filterElementType' ");
	    			}
    			}
    			
				/** actionTimeBuffer */
    			if (firstElement.hasAttribute("actionTimeBuffer")) {
        			try {
        				actionTimeBuffer = Long.parseLong(firstElement.getAttribute("actionTimeBuffer"));
        			} catch (Exception ex) {
        				log.warn("Cannot parse setting 'actionTimeBuffer' (data type: Long)");
        			}
    			}
				
    			/**
    			 * Parse filter tags
    			 */
				filterTag = new HashMap<String, List<String>>();
    			try {
	    			//feature types
    				NodeList nodeList1 = firstElement.getElementsByTagName("filterTag");
    				for(int t=0; t<nodeList1.getLength() ; t++) {
    	                Node node1 = nodeList1.item(t);
    	                if (node1.getNodeType() != Node.ELEMENT_NODE) continue;
						Element firstElement1 = (Element) node1;
						String key = firstElement1.getAttribute("key");
						if (!filterTag.containsKey(key)) {
							filterTag.put(key, new ArrayList<String>());
						}
						if (!firstElement1.getAttribute("value").equals("")) {
							filterTag.get(key).add(firstElement1.getAttribute("value"));
						}
    				}
    			} catch (Exception ex) {
    				log.warn("Cannot parse setting 'filterTag'");
    			}
    			
    			/**
    			 * Parse feature types
    			 */
				NodeList nodeList1 = firstElement.getElementsByTagName("featureType");
				for(int t=0; t<nodeList1.getLength() ; t++) {
	                Node node1 = nodeList1.item(t);
	                if (node1.getNodeType() != Node.ELEMENT_NODE) continue;
	                
					Element firstElement1 = (Element) node1;
					IVgiFeatureType vgiFeatureType = new VgiFeatureTypeImpl();
					
					SimpleFeatureTypeBuilder featureType = new SimpleFeatureTypeBuilder();
					featureType.setName(firstElement1.getAttribute("name"));

					featureType.setCRS(DefaultGeographicCRS.WGS84);
	    			try {
	    				switch (firstElement1.getAttribute("geometryType")) {
	    				case "Point":
	    					featureType.add("geom", Point.class);
	    					break;
	    				case "LineString":
	    					featureType.add("geom", LineString.class);
	    					break;
	    				case "Polygon":
	    					featureType.add("geom", Polygon.class);
	    					break;
	    				default:
	    					log.warn("Cannot parse setting 'featureType>geometryType'");
	    				}
	    			} catch (Exception ex) {
	    				log.warn("Cannot parse setting 'featureType>geometryType' ");
	    			}
	    			featureType.setDefaultGeometry("geom");
	    			
	    			featureType.add("osm_id", Long.class);
	    			featureType.add("deleted", Boolean.class);
	    			
	    			/** feature type tags **/
    				NodeList nodeList2 = firstElement1.getElementsByTagName("filterTag");
    				for(int v=0; v<nodeList2.getLength() ; v++) {
    	                Node node2 = nodeList2.item(v);
    	                if (node2.getNodeType() != Node.ELEMENT_NODE) continue;
						Element firstElement2 = (Element) node2;
						String key = firstElement2.getAttribute("key");
						if (!vgiFeatureType.getFeatureTypeTags().containsKey(key)) {
							vgiFeatureType.getFeatureTypeTags().put(key, new ArrayList<String>());
						}
						if (!firstElement2.getAttribute("value").equals("")) {
							vgiFeatureType.getFeatureTypeTags().get(key).add(firstElement2.getAttribute("value"));
						}
    				}
    				
	    			/** Property tags **/
    				NodeList nodeList3 = firstElement1.getElementsByTagName("property");
    				for(int v=0; v<nodeList3.getLength() ; v++) {
    	                Node node2 = nodeList3.item(v);
    	                if (node2.getNodeType() != Node.ELEMENT_NODE) continue;
						Element firstElement2 = (Element) node2;
						String key = firstElement2.getAttribute("key");
						featureType.add(key, String.class);
    				}
    				
//    				if (featureTypeList.containsKey(vgiFeatureType.getName())) {
//    					log.error("Feature Type with name '" + vgiFeatureType.getName() + "' already exists!");
//    				}
    				
    				vgiFeatureType.setFeatureType(featureType.buildFeatureType());
    				featureTypeList.put(featureType.getName(), vgiFeatureType);
    				log.info("FeatureType '" + featureType.getName() + "' added");
	            }
    			
    			/**
    			 * Parse analysis subjects
    			 */
				actionAnalyzerList = new ArrayList<IVgiAnalysisAction>();
				operationAnalyzerList = new ArrayList<IVgiAnalysisOperation>();
				featureAnalyzerList = new ArrayList<IVgiAnalysisFeature>();
				nodeList1 = firstElement.getElementsByTagName("analysisSubject");
				for(int t=0; t<nodeList1.getLength() ; t++) {
	                Node node1 = nodeList1.item(t);
	                if (node1.getNodeType() != Node.ELEMENT_NODE) continue;
					Element firstElement1 = (Element) node1;
					if (firstElement1.getAttribute("name").equals("VgiAnalysisUserPerAction")) {
						
						VgiAnalysisUserPerAction a = new VgiAnalysisUserPerAction(this);
						
	    				NodeList nodeList2 = firstElement1.getElementsByTagName("analysisSetting");
	    				if ((nodeList2.getLength() > 0) && (nodeList2.item(0).getNodeType() == Node.ELEMENT_NODE)) {
			    			try {
			    				a.setMergeActionTypes(Boolean.parseBoolean(((Element) nodeList2.item(0)).getAttribute("value")));
			    			} catch (Exception ex) {
			    				log.warn("Cannot parse setting 'VgiAnalysisUserPerAction>mergeActionTypes' (data type: Boolean)");
			    			}
	    				}

						actionAnalyzerList.add(a);
						
					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisActionPerType")) {
						actionAnalyzerList.add(new VgiAnalysisActionPerType(this));
					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisUpdate")) {
						actionAnalyzerList.add(new VgiAnalysisUpdate(this));
					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisUpdateGeometry")) {
						operationAnalyzerList.add(new VgiAnalysisUpdateGeometry());
					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisChangeDetection")) {
						actionAnalyzerList.add(new VgiAnalysisChangeDetection());
					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisUserPerOperation")) {
						
						VgiAnalysisUserPerOperation a = new VgiAnalysisUserPerOperation(this);
						
	    				NodeList nodeList2 = firstElement1.getElementsByTagName("analysisSetting");
	    				if ((nodeList2.getLength() > 0) && (nodeList2.item(0).getNodeType() == Node.ELEMENT_NODE)) {
			    			try {
			    				a.setMergeOperationTypes(Boolean.parseBoolean(((Element) nodeList2.item(0)).getAttribute("value")));
			    			} catch (Exception ex) {
			    				log.warn("Cannot parse setting 'VgiAnalysisUserPerOperation>mergeOperationTypes' (data type: Boolean)");
			    			}
	    				}

	    				operationAnalyzerList.add(a);
	    				
					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisOperationPerType")) {
						operationAnalyzerList.add(new VgiAnalysisOperationPerType());
						
					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisActionPerFeatureType")) {
						actionAnalyzerList.add(new VgiAnalysisActionPerFeatureType(this));
						
					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisTags")) {
						VgiAnalysisTags a = new VgiAnalysisTags(this);
						
	    				NodeList nodeList2 = firstElement1.getElementsByTagName("analysisSetting");
	    				if ((nodeList2.getLength() > 0) && (nodeList2.item(0).getNodeType() == Node.ELEMENT_NODE)) {
	    					a.setTagKey(((Element) nodeList2.item(0)).getAttribute("value"));
	    				}

	    				operationAnalyzerList.add(a);
						
					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisUserPerTags")) {
						VgiAnalysisUserPerTags a = new VgiAnalysisUserPerTags(this);
						
	    				NodeList nodeList2 = firstElement1.getElementsByTagName("analysisSetting");
	    				if ((nodeList2.getLength() > 0) && (nodeList2.item(0).getNodeType() == Node.ELEMENT_NODE)) {
	    					a.setTagKey(((Element) nodeList2.item(0)).getAttribute("value"));
	    				}

	    				operationAnalyzerList.add(a);

					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisFeatureStability")) {
						featureAnalyzerList.add(new VgiAnalysisFeatureStability(this));
					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisGeometryType")) {
						actionAnalyzerList.add(new VgiAnalysisGeometryType());
					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisHourOfDay")) {
						actionAnalyzerList.add(new VgiAnalysisHourOfDay());
					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisActionDetails")) {
						VgiAnalysisActionDetails a = new VgiAnalysisActionDetails(this);
						
	    				NodeList nodeList2 = firstElement1.getElementsByTagName("analysisSetting");
	    				if ((nodeList2.getLength() > 0) && (nodeList2.item(0).getNodeType() == Node.ELEMENT_NODE)) {
			    			try {
			    				a.setIncludeOperationDetails(Boolean.parseBoolean(((Element) nodeList2.item(0)).getAttribute("value")));
			    			} catch (Exception ex) {
			    				log.warn("Cannot parse setting 'VgiAnalysisActionDetails>includeOperationDetails' (data type: Boolean)");
			    			}
	    				}

						actionAnalyzerList.add(a);

					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisBatchGeneral")) {
						actionAnalyzerList.add(new VgiAnalysisBatchGeneral(this));
					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisBatchContributor")) {
						actionAnalyzerList.add(new VgiAnalysisBatchContributor(this));
					} else if (firstElement1.getAttribute("name").equals("VgiAnalysisBatchUserActionType")) {
						actionAnalyzerList.add(new VgiAnalysisBatchUserActionType(this));
					}
				}
    			
				/**
				 * Parse actions definitions
				 */
				actionDefinitionList = new ArrayList<IVgiAction>();
    			try {
    				nodeList1 = firstElement.getElementsByTagName("actionDefinition");
    				for(int t=0; t<nodeList1.getLength() ; t++) {
    	                Node node1 = nodeList1.item(t);
    	                if (node1.getNodeType() != Node.ELEMENT_NODE) continue;
						Element firstElement1 = (Element) node1;
						
						IVgiAction action = new VgiActionImpl();
						try {
							action.setActionName(firstElement1.getAttribute("name"));
							if (!firstElement1.getAttribute("geometryType").equals("")) {
								action.setGeometryType(VgiGeometryType.valueOf(firstElement1.getAttribute("geometryType")));
							}
						} catch(Exception ex) {
							log.warn("Cannot parse setting 'action definition': " + firstElement1.getAttribute("name"));
						}
		    			try {
		    				action.setActionType(VgiActionImpl.ActionType.valueOf(firstElement1.getAttribute("actionType")));
		    			} catch (Exception ex) {
		    				log.warn("Cannot parse setting 'actionType' ");
		    			}
						
	    				NodeList nodeList2 = firstElement1.getElementsByTagName("definitionRule");
	    				for(int v=0; v<nodeList2.getLength() ; v++) {
	    	                Node node2 = nodeList2.item(v);
	    	                if (node2.getNodeType() != Node.ELEMENT_NODE) continue;
    						Element firstElement2 = (Element) node2;
    						try {
    							action.addDefinitionRule(new VgiActionDefinitionRule(VgiOperationType.valueOf(firstElement2.getAttribute("operationType")), VgiActionDefinitionRule.EntryPointType.valueOf(firstElement2.getAttribute("entryPoint"))));
    						} catch(Exception ex) {
    							log.warn("Cannot parse setting 'definitionRule': " + firstElement1.getAttribute("name") + ">" + firstElement2.getAttribute("operationType"));
    						}
	    				}
	    				
	    				Collections.sort(action.getDefinition(), VgiActionDefinitionRule.getOperationTypeComparator());
	    				Collections.reverse(action.getDefinition());
	    				
	    				actionDefinitionList.add(action);
    				}
    			} catch (Exception ex) {
    				log.warn("Cannot parse setting 'actionDefinitions'");
    			}
    			
            }

            /**
			 * Parse filter polygons
			 */
            nodeList = doc.getElementsByTagName("filterPolygons");
            
            for(int s=0; s<nodeList.getLength(); s++) {
            	if (filterPolygonList == null) filterPolygonList = new ArrayList<VgiPolygon>();
            	
                Node node = nodeList.item(s);
				if (node.getNodeType() != Node.ELEMENT_NODE) continue; 
					
				Element firstElement = (Element) node;
				
				NodeList nodeList1 = firstElement.getElementsByTagName("polygon");
				for(int t=0; t<nodeList1.getLength() ; t++) {
	                Node node1 = nodeList1.item(t);
	                if (node1.getNodeType() != Node.ELEMENT_NODE) continue;
					Element firstElement1 = (Element) node1;
					VgiPolygon polygon = new VgiPolygon();
					if (!firstElement1.getAttribute("label").equals("")) {
	    				polygon.setLabel(firstElement1.getAttribute("label"));
					}
	    			try {
	    				if (!firstElement1.getAttribute("geometry").equals("")) {
		    				WKTReader wktReader = new WKTReader();
		    				polygon.setPolygon((Polygon)wktReader.read(firstElement1.getAttribute("geometry")));
		    				filterPolygonList.add(polygon);
	    				}
	    				
	    			} catch (Exception ex) {
	    				log.warn("Cannot parse setting 'filterPolygon' (" + polygon.getLabel() + ")");
	    			}
				}
            }
            settingsLoaded = true;
	    } catch (ParserConfigurationException e) {
			e.printStackTrace();
	    } catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	    
	    log.info("Settings '" + this.getSettingName() + "' loaded");
	}


	@Override
	public String getSettingName() {
		return settingName;
	}

	@Override
	public void setSettingName(String name) {
		settingName = name;
	}

	@Override
	public File getPbfDataFolder() {
		return pbfDataFolder;
	}
	@Override
	public void setPbfDataFolder(File pbfDataFolder) {
		this.pbfDataFolder = pbfDataFolder;
		if (!pbfDataFolder.exists()) {
			log.error("Cannot find pbfDataFolder!");
			System.exit(1);
		}
	}

	@Override
	public boolean isReadQuadtree() {
		return readQuadtree;
	}
	@Override
	public void setReadQuadtree(boolean readQuadtree) {
		this.readQuadtree = readQuadtree;
	}

	@Override
	public File getResultFolder() {
		return resultFolder;
	}

	@Override
	public boolean isIgnoreFeaturesWithoutTags() {
		return ignoreFeaturesWithoutTags;
	}

	@Override
	public Date getAnalysisStartDate() {
		return analysisStartDate;
	}

	@Override
	public Date getAnalysisEndDate() {
		return analysisEndDate;
	}

	@Override
	public boolean isFindRelatedOperations() {
		return findRelatedOperations;
	}
	
	@Override
	public boolean isWriteGeometryFiles() {
		return writeGeometryFiles;
	}
	
	@Override
	public boolean isIgnoreNonFeatureTypeTags() {
		return ignoreNonFeatureTypeTags;
	}

	@Override
	public long getFilterUid() {
		return filterUid;
	}
	
	@Override
	public String getTemporalResolution() {
		return temporalResolution;
	}

	@Override
	public Date getFilterTimestamp() {
		return filterTimestamp;
	}

	@Override
	public VgiGeometryType getFilterElementType() {
		return filterElementType;
	}
	
	@Override
	public Map<String, List<String>> getFilterTag() {
		return filterTag;
	}

	@Override
	public List<VgiPolygon> getFilterPolygonList() {
		return filterPolygonList;
	}
	@Override
	public void setFilterPolygonList(List<VgiPolygon> filterPolygonList) {
		this.filterPolygonList = filterPolygonList;
	}

	@Override
	public VgiPolygon getFilterPolygon() {
		return filterPolygon;
	}
	@Override
	public void setFilterPolygon(VgiPolygon polygon) {
		this.filterPolygon = polygon;
	}
	
	@Override
	public long getActionTimeBuffer() {
		return actionTimeBuffer;
	}
	
	@Override
	public List<IVgiAction> getActionDefinitionList() {
		return actionDefinitionList;
	}

	@Override
	public List<IVgiAnalysisAction> getActionAnalyzerList() {
		return this.actionAnalyzerList;
	}

	@Override
	public List<IVgiAnalysisOperation> getOperationAnalyzerList() {
		return this.operationAnalyzerList;
	}

	@Override
	public List<IVgiAnalysisFeature> getFeatureAnalyzerList() {
		return this.featureAnalyzerList;
	}
	
	@Override
	public Map<String, IVgiFeatureType> getFeatureTypeList() {
		return featureTypeList;
	}

	@Override
	public Map<String, List<IVgiFeature>> getCache() {
		return cache;
	}
	@Override
	public void setCache(Map<String, List<IVgiFeature>> cache) {
		this.cache = cache;
	}

	@Override
	public int getKeepInCacheLevel() {
		return keepInCacheLevel;
	}
	@Override
	public void setKeepInCacheLevel(int keepInCacheLevel) {
		this.keepInCacheLevel = keepInCacheLevel;
	}
}
