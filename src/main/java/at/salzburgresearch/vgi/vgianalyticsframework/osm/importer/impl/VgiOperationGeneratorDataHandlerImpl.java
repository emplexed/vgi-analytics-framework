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

package at.salzburgresearch.vgi.vgianalyticsframework.osm.importer.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.vividsolutions.jts.geom.Coordinate;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.IOsmElement;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Node;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Relation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Way;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiFeatureImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiPolygon;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.IVgiOperationPbfWriter;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipeline;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.impl.ReadAllFeaturesConsumer;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.importer.OsmDataConsumer;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service.IOsmVgiOperationGenerator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;

public class VgiOperationGeneratorDataHandlerImpl implements OsmDataConsumer, ApplicationContextAware {
	private static Logger log = Logger.getLogger(VgiOperationGeneratorDataHandlerImpl.class);
	
	private ApplicationContext ctx;
	
	private IVgiPipelineSettings settings = null;
	
	private IOsmVgiOperationGenerator vgiOperationGenerator = null;
	private IVgiOperationPbfWriter vgiOperationPbfWriter = null;
	
	private IVgiFeature currentFeature = null;
	private List<IVgiFeature> featureList = new ArrayList<IVgiFeature>();
	private TLongArrayList childNodeList = new TLongArrayList();
	private TLongArrayList childWayList = new TLongArrayList();
	private TLongArrayList childRelationList = new TLongArrayList();
	
	private enum Phases {BEFORE, NODES, WAYS, RELATIONS, AFTER};
	private Phases currentPhase = Phases.BEFORE;
	
	private long numParsedNodes = 0l;
	private long numParsedWays = 0l;
	private long numParsedRelations = 0l;
	
	private IOsmElement lastOsmElement = null;
	
	public VgiOperationGeneratorDataHandlerImpl (IVgiPipelineSettings settings) {
		this.settings = settings;
	}
	
	@Override
	public void beforeProcessing() {
		log.info("Start reading PBF data...");
	}
	
	@Override
	public void process(IOsmElement osmElement) {
		if (lastOsmElement != null) {
			if (osmElement.getId() != lastOsmElement.getId() || 
					!osmElement.getElementType().equals(lastOsmElement.getElementType())) {
				lastOsmElement = null;
			}
		}
		switch (osmElement.getElementType()) {
		case NODE:
			if (!currentPhase.equals(Phases.NODES)) {
				vgiOperationPbfWriter.initializePbfWriterToAppend(settings.getPbfDataFolder());
				log.info("Output path: " + settings.getPbfDataFolder());
				log.info("Start node phase");
				currentPhase = Phases.NODES;
			}
			enqueueOperations(vgiOperationGenerator.generateNodeOperations((Node)osmElement, (Node)lastOsmElement));
			numParsedNodes++;
			break;
			
		case WAY:
			if (!currentPhase.equals(Phases.WAYS)) {
				flushQueue();
				vgiOperationPbfWriter.terminatePbfWriter();
				vgiOperationPbfWriter.initializePbfWriterToAppend(settings.getPbfDataFolder());
				log.info("Start way phase");
				currentPhase = Phases.WAYS;
			}
			enqueueOperations(vgiOperationGenerator.generateWayOperations((Way)osmElement, (Way)lastOsmElement));
			numParsedWays++;
			break;
			
		case RELATION:
			if (!currentPhase.equals(Phases.RELATIONS)) {
				flushQueue();
				vgiOperationPbfWriter.terminatePbfWriter();
				vgiOperationPbfWriter.initializePbfWriterToAppend(settings.getPbfDataFolder());
				log.info("Start relation phase");
				currentPhase = Phases.RELATIONS;
			}
			enqueueOperations(vgiOperationGenerator.generateRelationOperations((Relation)osmElement, (Relation)lastOsmElement));
			numParsedRelations++;
			break;
			
		default:
			break;
		}
		lastOsmElement = osmElement;
	}

	@Override
	public void afterProcessing() {
		currentPhase = Phases.AFTER;
		/** Flush queue */
		flushQueue();
		/** Terminate PBF writer */
		this.vgiOperationPbfWriter.terminatePbfWriter();
	}
	
	/**
	 * Adds operations to operation queue. If operation queue has reached the limit, flush it. 
	 * @param List with VgiOperations of one feature
	 */
	private void enqueueOperations(List<IVgiOperation> operationList) {
		if (operationList == null || operationList.size() == 0) return;
		
		/** Process feature */
		if (currentFeature != null 
				&& (currentFeature.getOid() != operationList.get(0).getOid()
				|| !currentFeature.getVgiGeometryType().equals(operationList.get(0).getVgiGeometryType()))) {
			
			/** Add all nodes and filtered ways/relations to feature list */
			boolean filterByTag = currentFeature.filterByTag(settings.getFilterTag());
			if (currentFeature.getVgiGeometryType().equals(VgiGeometryType.POINT) || filterByTag) {
				if (!filterByTag) {
					currentFeature.stripFeature();
				}
				featureList.add(currentFeature);
				
				/** Retrieve children elements */
				for (IVgiOperation operation : currentFeature.getOperationList()) {
					if (operation.getVgiOperationType().equals(VgiOperationType.OP_ADD_NODE)) {
						childNodeList.add(operation.getRefId());
					} else if (operation.getVgiOperationType().equals(VgiOperationType.OP_ADD_MEMBER)) {
//						if (operation.getKey().equals("n")) {
//							childNodeList.add(operation.getRefId());
//						} else if (operation.getKey().equals("w")) {
//							childWayList.add(operation.getRefId());
//						} else if (operation.getKey().equals("r")) {
//							childRelationList.add(operation.getRefId());
//						}
					}
				}
			}
			currentFeature = null;
			if (featureList.size() >= 300000 || childNodeList.size() >= 3000000 || 
					Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() + Runtime.getRuntime().freeMemory() < 1024*1024*1024*4) {
				try {
					log.info("Sleep for 0.2 second...");
					Thread.sleep(200); //TODO test if this is still necessary
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				log.debug("Memory: Free=" + Runtime.getRuntime().freeMemory() + "; Total="
						+ Runtime.getRuntime().totalMemory() + " Max=" + Runtime.getRuntime().maxMemory());
				log.info("#Parsed: " + numParsedNodes + "n|" + numParsedWays + "w|" + numParsedRelations
						+ "r #Features: " + featureList.size() + " - #ChildNodes: " + childNodeList.size());
				flushQueue();
			}
		}
		/** Create new feature */
		if (currentFeature == null) {
			currentFeature = new VgiFeatureImpl();
			currentFeature.setOid(operationList.get(0).getOid());
			currentFeature.setVgiGeometryType(operationList.get(0).getVgiGeometryType());
		}
		
		/** Add operations to current feature */
		currentFeature.getOperationList().addAll(operationList);
	}
	
	/**
	 * The operations in the queue are extended by coordinates and sent to the PBF writer
	 */
	private void flushQueue() {
		/** Add last feature to queue */
		if (currentFeature != null) featureList.add(currentFeature);
		
		log.info("Flush " + featureList.size() + " features with " + childNodeList.size() + " child nodes");
		
//		if (childNodeList.size() > 0 || childWayList.size() > 0 || childRelationList.size() > 0) {
		if (childNodeList.size() > 0) {
			/** Find coordinates using a VGI pipeline */
			childNodeList.sort();
			childWayList.sort();
			childRelationList.sort();
			
			log.info("Find coordinates for " + childNodeList.size() + " child nodes - Start");
			IVgiPipeline pipeline = ctx.getBean("getFeaturesPipeline", IVgiPipeline.class);
			pipeline.setPbfDataFolder(settings.getPbfDataFolder());
			pipeline.setFilterNodeId(childNodeList);
			pipeline.setFilterWayId(childWayList);
			pipeline.setFilterRelationId(childRelationList);
			pipeline.start();
			
			log.debug("Find coordinates for " + childNodeList.size() + " child nodes - End");
			childNodeList.clear();
			childNodeList.trimToSize();
			childWayList.clear();
			childWayList.trimToSize();
			childRelationList.clear();
			childRelationList.trimToSize();
			
			/** Add geometries to operations */
			log.info("Add coordinates to operations - Start");
			addCoordinateToOperations(((ReadAllFeaturesConsumer)pipeline.getConsumers().get(0)).getFeatureList());
			log.debug("Add coordinates to operations - End");
		}
		
		if (settings.getFilterPolygonList() != null && !settings.getFilterPolygonList().isEmpty()) {
			List<IVgiFeature> filteredFeatureList = new ArrayList<IVgiFeature>();
			for (IVgiFeature feature : featureList) {
				boolean withinBBox = false;
				for (IVgiOperation operation : feature.getOperationList()) {
					for (VgiPolygon polygon : settings.getFilterPolygonList()) {
						if (operation.getCoordinate() != null && polygon.getPolygon().getEnvelopeInternal().contains(operation.getCoordinate())) {
							withinBBox = true;
							break;
						}
					}
				}
				if (withinBBox || feature.getVgiGeometryType().equals(VgiGeometryType.RELATION)) {
					filteredFeatureList.add(feature);
				}
			}
			log.info("Polygon filter: " + filteredFeatureList.size() + " of " + featureList.size() + " features found!");
			featureList = filteredFeatureList;
		}
		
		if (featureList.size() > 0) {
			log.info("Write " + featureList.size() + " features to PBF files - Start");
			vgiOperationPbfWriter.writePbfFeatures(featureList);
			log.debug("Write " + featureList.size() + " features to PBF files - End");
		}
		
		featureList = new ArrayList<IVgiFeature>();
		currentFeature = null;
	}
	
	/**
	 * Adds coordinates to operations (OpAddNode, OpModifyWayCoordinate)
	 * @param refElements list of child elements (way nodes and relation members)
	 */
	private void addCoordinateToOperations(List<IVgiFeature> refElements) {
		
		log.info(refElements.size() + " ref elements found!");
		
		refElements.sort(VgiFeatureImpl.getFeatureComparator());
		
		int numFound=0, numNotFound=0;
		
		for (IVgiFeature feature : featureList) {
			
			TLongObjectHashMap<List<IVgiOperation>> childElementHistories = new TLongObjectHashMap<List<IVgiOperation>>();
			TLongObjectHashMap<List<IVgiOperation>> featureChildElements = new TLongObjectHashMap<List<IVgiOperation>>();
			
			/** Add coordinates to OpAddNode operations */
			for (IVgiOperation featureOperation : feature.getOperationList()) {
				
				/** Create childElementHistory (will be used for adding OpModifyWayCoordinate) */
				if (featureOperation.getVgiOperationType().equals(VgiOperationType.OP_ADD_NODE)) {
//						|| featureOperation.getVgiOperationType().equals(VgiOperationType.OP_REMOVE_NODE)
//						|| featureOperation.getVgiOperationType().equals(VgiOperationType.OP_ADD_MEMBER)
//						|| featureOperation.getVgiOperationType().equals(VgiOperationType.OP_REMOVE_MEMBER)) {
					if (!childElementHistories.containsKey(featureOperation.getRefId())) {
						/** Initialize child element history */
						childElementHistories.put(featureOperation.getRefId(), new ArrayList<IVgiOperation>());
					}
					childElementHistories.get(featureOperation.getRefId()).add(featureOperation);
				}
				
				/** Only OpAddNode (and OpAddMember) */
				if (!featureOperation.getVgiOperationType().equals(VgiOperationType.OP_ADD_NODE)) continue;
				
				/** Find ref element */
				IVgiFeature refElement = null;
				IVgiFeature refElementForSearch = new VgiFeatureImpl();
				refElementForSearch.setOid(featureOperation.getRefId());
				if (featureOperation.getVgiOperationType().equals(VgiOperationType.OP_ADD_NODE)) {
					refElementForSearch.setVgiGeometryType(VgiGeometryType.POINT);
//				} else if (featureOperation.getVgiOperationType().equals(VgiOperationType.OP_ADD_MEMBER)) {
//					if (featureOperation.getKey().equals("n")) {
//						refElementForSearch.setVgiGeometryType(VgiGeometryType.POINT);
//					} else if (featureOperation.getKey().equals("w")) {
//						refElementForSearch.setVgiGeometryType(VgiGeometryType.LINE);
//					} else if (featureOperation.getKey().equals("r")) {
//						refElementForSearch.setVgiGeometryType(VgiGeometryType.RELATION);
//					}
				}
				int index = Collections.binarySearch(refElements, refElementForSearch, VgiFeatureImpl.getFeatureComparator());
				if (index >= 0) {
					refElement = refElements.get(index);
					numFound++;
				} else {
					numNotFound++;
					if (settings.getCurrentPolygon() == null) {
						/** only write error if no polygon filter is set */
						if (numNotFound < 100 || numNotFound % 10000 == 0) {
							log.error(featureOperation.getVgiGeometryType() + "/" + featureOperation.getOid() + "/v"
									+ featureOperation.getVersion() + ": Cannot find ref element "
									+ refElementForSearch.getVgiGeometryType() + "/" + refElementForSearch.getOid()
									+ " (" + numNotFound + "/" + (numFound + numNotFound) + ")");
						}
					}
					continue;
				}
				
				/** Operations for added nodes */
				featureChildElements.put(refElement.getOid(), refElement.getOperationList());
				
				/** Some nodes are added to way before they are created (e.g. node 20950647 in way 4848297) */
				/** we want to find a coordinate, also if the node has been created too late... */
				
				for (IVgiOperation childNodeOperation : refElement.getOperationList()) {
					
					/** only operations which have a coordinate */
					if (childNodeOperation.getCoordinate() == null) continue;
					
					/** Find latest node coordinate */
					/** if (coordinate not found yet OR operation timestamp not after changeset timestamp */
					if (featureOperation.getCoordinate() != null && childNodeOperation.getTimestamp().after(featureOperation.getTimestamp())) break;
						
					/** Add coordinate to operation */
					featureOperation.setCoordinate(new Coordinate(childNodeOperation.getCoordinate()));
				}
			}
			
			/** Add OpModifyWayCoordinate operations */
			for (long childElementId : featureChildElements.keys()) { /** Nodes which are part of way twice are processed once */
				
				/** Get operations and sort them */
				List<IVgiOperation> childElementHistory = childElementHistories.get(childElementId);
				List<IVgiOperation> childNodeOperations = featureChildElements.get(childElementId);
				
				Collections.sort(childElementHistory, VgiOperationImpl.getVersionComparator());
				Collections.sort(childNodeOperations, VgiOperationImpl.getVersionComparator());
				
				/** Add operation to feature if node is in node list at this timestamp */
				for (IVgiOperation nodeOperation : childNodeOperations) {
					
					/** Only OpModifyCoordinate */
					if (!nodeOperation.getVgiOperationType().equals(VgiOperationType.OP_MODIFY_COORDINATE)) continue;
					
					int membershipCount = 0;
					
					/** Iterate through OpAddNodes and OpRemoveNodes */
					for (int i=0; i<childElementHistory.size();i++) {
						
						/** We want the way node history before the node operation */
						if (childElementHistory.get(i).getTimestamp().after(nodeOperation.getTimestamp())) break;
						
						if (childElementHistory.get(i).getVgiOperationType().equals(VgiOperationType.OP_ADD_NODE)) membershipCount++;
						if (childElementHistory.get(i).getVgiOperationType().equals(VgiOperationType.OP_REMOVE_NODE)) membershipCount--;
					}
					
					/** Node should be member of way at least once */
					if (membershipCount == 0) continue;
					
					/** Add OP_UPDATE_WAY_COORDINATE */
					IVgiOperation newOperation = new VgiOperationImpl(feature.getOid(), feature.getVgiGeometryType(),
							VgiOperationType.OP_MODIFY_WAY_COORDINATE, nodeOperation.getUid(), nodeOperation.getUser(),
							nodeOperation.getTimestamp(), Short.MAX_VALUE, nodeOperation.getChangesetid(),
							nodeOperation.getCoordinate(), "", "", nodeOperation.getOid(), -1);

					/** Find index and version with feature operation list */
					int index = Collections.binarySearch(feature.getOperationList(), newOperation, VgiOperationImpl.getTimestampComparator());
					if (index >= 0) {
						/** Timestamp/OpType combination already exists */
						newOperation.setVersion(feature.getOperationList().get(index).getVersion());
						feature.getOperationList().add(index+1, newOperation);
					} else {
						index = (index+1) * -1;
						newOperation.setVersion(feature.getOperationList().get(index-1).getVersion());
						feature.getOperationList().add(index, newOperation);
					}
				}
			}
			
			/** Sort operations */
			Collections.sort(feature.getOperationList(), VgiOperationImpl.getVersionComparator());
		}
		
		log.info(numNotFound + " of " + (numNotFound + numFound) + " ref elements not found!");
	}
	
	public IOsmVgiOperationGenerator getVgiOperationGenerator() {
		return vgiOperationGenerator;
	}
	
	public void setVgiOperationGenerator(IOsmVgiOperationGenerator vgiOperationGenerator) {
		this.vgiOperationGenerator = vgiOperationGenerator;
	}
	
	public void setVgiOperationPbfWriter(IVgiOperationPbfWriter vgiOperationPbfWriter) {
		this.vgiOperationPbfWriter = vgiOperationPbfWriter;
	}

	@Override
	public void setApplicationContext(ApplicationContext ctx) throws BeansException {
		this.ctx = ctx;
	}
}
