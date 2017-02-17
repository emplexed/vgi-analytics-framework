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
import java.util.HashMap;
import java.util.Map;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.IOsmElement;

public abstract class OsmElement implements IOsmElement {

	protected OsmElementType elementType = OsmElementType.UNKOWN;
	protected long id = -1;
	protected int uid = -1;
	protected String username = "";
	protected Date timestamp = null;
	protected int changesetId = -1;
	protected short version = -1;
	protected boolean visible = false;
	protected Map<String, String> tags = null;

	public OsmElement(OsmElementType elementType, long id) {
		this.elementType = elementType;
		this.id = id;
		tags = new HashMap<String, String>();
	}
	
	public OsmElement(OsmElementType elementType, long id, int uid, String username, Date timestamp, int changesetId,
			short version, boolean visible, Map<String, String> tags) {
		this.elementType = elementType;
		this.id = id;
		this.uid = uid;
		this.username = username;
		this.timestamp = timestamp;
		this.changesetId = changesetId;
		this.version = version;
		this.visible = visible;
		this.tags = tags;
	}

	public OsmElementType getElementType() {
		return elementType;
	}
	
	public void setElementType(OsmElementType elementType) {
		this.elementType = elementType;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public int getUid() {
		return uid;
	}

	public void setUid(int uid) {
		this.uid = uid;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public int getChangesetId() {
		return changesetId;
	}

	public void setChangesetId(int changesetId) {
		this.changesetId = changesetId;
	}

	public short getVersion() {
		return version;
	}

	public void setVersion(short version) {
		this.version = version;
	}

	public boolean isVisible() {
		return visible;
	}

	public void setVisible(boolean visible) {
		this.visible = visible;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}
}
