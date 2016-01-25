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
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.vividsolutions.jts.geom.Coordinate;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.IOsmElement;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Node;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.OsmElementType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Relation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.RelationMember;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Way;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.IVgiPipelineConsumer;
import gnu.trove.list.array.TLongArrayList;

public class OsmElementBuilderConsumer implements IVgiPipelineConsumer {
	private static Logger log = Logger.getLogger(OsmElementBuilderConsumer.class);
	
	private Date cutoffDate = new Date();
	private final SimpleDateFormat dateFormatOSM = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	
	private List<IOsmElement> featureList = new ArrayList<IOsmElement>();

	public OsmElementBuilderConsumer() {
	}
	
	@Override
	public void doBeforeFirstBatch() { }
	
	@Override
	public void handleBatch(List<IVgiFeature> batch) {		
		log.info("Start building features from operation (Cutoff date: " + dateFormatOSM.format(cutoffDate) + ")");
		for (IVgiFeature feature : batch) {
			featureList.add(buildOsmEntry(feature));
		}
	}
	
	@Override
	public void doAfterLastBatch() { }
	
	public void setCutoffDate(Date d) {
		this.cutoffDate = d;
	}
	
	public List<IOsmElement> getFeatureList() {
		return featureList;
	}
	
	public IOsmElement buildOsmEntry(IVgiFeature feature) {
		IOsmElement osmFeature = null;
		
		TLongArrayList nodeList = new TLongArrayList();
		
		Collections.sort(feature.getOperationList(), VgiOperationImpl.getVersionComparator());
		
		for (IVgiOperation operation : feature.getOperationList()) {
			/** Build feature for a specific date */
			if (operation.getTimestamp().after(cutoffDate)) break;
			
			try {
				switch (operation.getVgiOperationType()) {
				case OP_CREATE_NODE:
				case OP_RECREATE_NODE:
					osmFeature = new Node(OsmElementType.NODE,
							operation.getOid(), operation.getUid(), "", new Date(-1), -1, (short)-1, true, new HashMap<String, String>(), 
							new Coordinate(operation.getCoordinate().x, operation.getCoordinate().y));
					break;
					
				case OP_CREATE_WAY:
				case OP_RECREATE_WAY:
					osmFeature = new Way(OsmElementType.WAY,
							operation.getOid(), operation.getUid(), "", new Date(-1), -1, (short)-1, true, new HashMap<String, String>(), 
							new ArrayList<Long>());
					break;
					
				case OP_CREATE_RELATION:
				case OP_RECREATE_RELATION:
					osmFeature = new Relation(OsmElementType.RELATION,
							operation.getOid(), operation.getUid(), "", new Date(-1), -1, (short)-1, true, new HashMap<String, String>(), 
							new ArrayList<RelationMember>());
					break;
					
				case OP_DELETE_NODE:
//					((Node) osmFeature).setPoint(null);
				case OP_DELETE_WAY:
				case OP_DELETE_RELATION:
					osmFeature.setVisible(false);
					break;
					
				case OP_ADD_NODE:
					nodeList.insert(operation.getPosition(), operation.getRefId());
					break;
					
				case OP_REMOVE_NODE:
					if (nodeList.size() == 0) {
						log.warn("OpRemoveNode operation for empty node list");
					} else {
						nodeList.remove(operation.getPosition());
					}
					break;
					
				case OP_REORDER_NODE:
					nodeList.insert(operation.getPosition(), nodeList.removeAt((int)operation.getRefId()));
					break;
					
				case OP_ADD_MEMBER:
					OsmElementType memberType = OsmElementType.NODE;
					if  (operation.getKey().equals("w")) {
						memberType = OsmElementType.WAY;
					} else if  (operation.getKey().equals("r")) {
						memberType = OsmElementType.RELATION;
					}
					RelationMember member = new RelationMember(operation.getRefId(), memberType, operation.getValue());
					
					((Relation) osmFeature).getMembers().add(operation.getPosition(), member);
					break;
					
				case OP_REMOVE_MEMBER:
					((Relation) osmFeature).getMembers().remove(operation.getPosition());
					break;
					
				case OP_REORDER_MEMBER:
					((Relation) osmFeature).getMembers().add(operation.getPosition(), ((Relation) osmFeature).getMembers().remove((int)operation.getRefId()));
					break;
					
				case OP_MODIFY_COORDINATE:
					((Node) osmFeature).getCoordinate().setCoordinate(new Coordinate(operation.getCoordinate().x, operation.getCoordinate().y));
					break;
					
				case OP_MODIFY_ROLE:
//					((Relation) osmFeature).getMembers().get(operation.getPosition()).setMemberRole(operation.getValue());//TODO
					break;
					
				case OP_ADD_TAG:
					try {
						osmFeature.getTags().put(operation.getKey(), operation.getValue());
					} catch (NullPointerException ex) {
						log.info("");
					}
					break;
					
				case OP_MODIFY_TAG_VALUE:
//					osmFeature.getTags().put(operation.getKey(), operation.getValue());//TODO
					break;
					
				case OP_REMOVE_TAG:
					osmFeature.getTags().remove(operation.getKey());
					break;
					
				default:
					break;
				}
			} catch (Exception e) {
				log.info(e.getMessage());
			}
			
			if (osmFeature != null) osmFeature.setTimestamp(operation.getTimestamp());
		}
		
		/** Add nodes to way */
		if (nodeList.size() > 0) {
			for (int i=0; i<nodeList.size(); i++) {
				((Way) osmFeature).getWayNodes().add(nodeList.get(i));
			}
		}
		
		return osmFeature;
	}
}
