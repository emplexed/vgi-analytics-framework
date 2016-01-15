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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm;

import java.util.Date;
import java.util.Map;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.OsmElementType;

public interface IOsmElement {
	public OsmElementType getElementType();
	public void setElementType(OsmElementType elementType);
	
	public long getId();
	public void setId(long id);

	public int getUid();
	public void setUid(int uid);

	public String getUsername();
	public void setUsername(String username);

	public Date getTimestamp();
	public void setTimestamp(Date timestamp);

	public int getChangesetId();
	public void setChangesetId(int changesetId);

	public short getVersion();
	public void setVersion(short version);

	public boolean isVisible();
	public void setVisible(boolean visible);

	public Map<String, String> getTags();
	public void setTags(Map<String, String> tags);
}
