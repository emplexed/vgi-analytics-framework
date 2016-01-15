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

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.opengis.feature.simple.SimpleFeatureType;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiContributorImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;

public class VgiAnalysisUser extends VgiContributorImpl {
	public Map<String, Map<Date, Integer>> actionCount = new ConcurrentHashMap<String, Map<Date, Integer>>();
	public Map<VgiOperationType, Map<Date, Long>> operationCount = new ConcurrentHashMap<VgiOperationType, Map<Date, Long>>();
	public Map<String, long[]> addedTagPerKey = new ConcurrentHashMap<String, long[]>(); /** Map<tag_key, [#node, #way, #relation]> */
	public Map<String, long[]> modifiedTagPerKey = new ConcurrentHashMap<String, long[]>(); /** Map<tag_key, [#node, #way, #relation]> */
	public Map<String, long[]> removedTagPerKey = new ConcurrentHashMap<String, long[]>(); /** Map<tag_key, [#node, #way, #relation]> */
	public Map<Integer, Long> actionTimestampPerHour = new ConcurrentHashMap<Integer, Long>(); /** Map<Hour, #actions> */
	public Map<Date, Map<SimpleFeatureType, Integer>> actionPerFeatureType = new ConcurrentHashMap<Date, Map<SimpleFeatureType, Integer>>();
	/** Who modifies/deletes features which have been created by user x? */
	public Map<String, Long[]> whoModifiedMyFeature = new ConcurrentHashMap<String, Long[]>(); /** Map<Action, [#create_user, #other user]> */
	
	public double createdWayDistance = 0.0;
	
	public VgiAnalysisUser (int uid, String username) {
		super(uid, username);
	}
}
