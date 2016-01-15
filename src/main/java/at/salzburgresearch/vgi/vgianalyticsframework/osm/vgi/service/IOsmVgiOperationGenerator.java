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

package at.salzburgresearch.vgi.vgianalyticsframework.osm.vgi.service;

import java.util.List;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Node;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.OsmElement;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Relation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Way;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;

public interface IOsmVgiOperationGenerator {
	/**
	 * Extract operation from a OSM node entity (compared to another one)
	 * 
	 * @return void
	 */
	public List<IVgiOperation> generateNodeOperations(Node value, Node oldValue);
	
	/**
	 * Extract operation from a OSM way entity (compared to another one)
	 * 
	 * @return void
	 */
	public List<IVgiOperation> generateWayOperations(Way value, Way oldValue);
	
	/**
	 * Extract operation from a OSM relation entity (compared to another one)
	 * 
	 * @return void
	 */
	public List<IVgiOperation> generateRelationOperations(Relation value, Relation oldValue);

	/**
	 * Extract tag operations from a OSM feature
	 * 
	 * @return a list of created IVgiOperations
	 */
	List<IVgiOperation> compareTags (OsmElement value, OsmElement oldValue);

	/**
	 * Extract deleted operations from a OSM feature
	 * 
	 * @return a list of created IVgiOperations
	 */
	List<IVgiOperation> checkIfDeleted (OsmElement value, OsmElement oldValue);
	
	public IOsmVgiNodeOperationGenerator getNodeOpGenerator();
	
	public void setNodeOpGenerator(IOsmVgiNodeOperationGenerator nodeOpGenerator);
	
	public IOsmVgiWayOperationGenerator getWayOpGenerator();
	
	public void setWayOpGenerator(IOsmVgiWayOperationGenerator wayOpGenerator);
	
	public IOsmVgiRelationOperationGenerator getRelationOpGenerator();
	
	public void setRelationOpGenerator(IOsmVgiRelationOperationGenerator relationOpGenerator);

}
