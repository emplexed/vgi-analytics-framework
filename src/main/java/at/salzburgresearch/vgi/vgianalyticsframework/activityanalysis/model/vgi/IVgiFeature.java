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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi;

import java.util.List;
import java.util.Map;

import com.vividsolutions.jts.geom.Envelope;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiFeatureImpl.LocalizeType;

public interface IVgiFeature extends IVgiBaseObject {
	
	boolean filterByTag(Map<String, List<String>> filterTag);

	List<IVgiOperation> getOperationList();
	void setOperationList(List<IVgiOperation> operationList);
	void addOperation(IVgiOperation operation);
	
	List<IVgiAction> getActionList();
	void setActionList(List<IVgiAction> actionList);
	void addAction(IVgiAction action);

	Envelope getBBox();
	void setBBox(Envelope bbox);
	
	String getQuadtreePath();
	void setQuadtreePath(String quadtreePath);
	
	LocalizeType getLocalizeType();
	void setLocalizeType(LocalizeType localizeType);

	List<IVgiFeature> getRelationMembers();
	void setRelationMembers(List<IVgiFeature> relationMembers);
}
