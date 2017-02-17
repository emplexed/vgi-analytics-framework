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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl;

import java.util.Date;
import java.util.Map;

import com.vividsolutions.jts.geom.Coordinate;

public class Node extends OsmElement {

	private Coordinate coordinate = null;

	public Node(long id) {
		super(OsmElementType.NODE, id);
	}
	public Node(OsmElementType elementType, long id, int uid, String username, Date timestamp, int changesetId,
			short version, boolean visible, Map<String, String> tags, Coordinate coordinate) {
		super(elementType, id, uid, username, timestamp, changesetId,version, visible, tags);
		this.coordinate = coordinate;
	}
	
	public Coordinate getCoordinate() {
		return coordinate;
	}
	public void setCoordinate(Coordinate coordinate) {
		this.coordinate = coordinate;
	}
}
