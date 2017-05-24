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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;

/**
 * Parent class for all analysis classes
 *
 */
public abstract class VgiAnalysisParent {
	protected static Logger log = org.apache.logging.log4j.LogManager.getLogger(VgiAnalysisParent.class);
	
	protected IVgiPipelineSettings settings = null;

	protected final SimpleDateFormat dateFormatYear = new SimpleDateFormat("yyyy");
	protected final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	protected final SimpleDateFormat dateFormatOSM = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	protected final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected final DecimalFormat decimalFormat = new DecimalFormat("0.0#####");

	protected static Map<Integer, VgiAnalysisUser> userAnalysis = new ConcurrentHashMap<Integer, VgiAnalysisUser>();
	private static VgiAnalysisUser previousUser = null;
	
	protected long processingTime = 0;

	public VgiAnalysisParent() {
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		dateFormatOSM.setTimeZone(TimeZone.getTimeZone("UTC"));
		dateTimeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	
	public static VgiAnalysisUser findUser(int uid, String username) {
		/** Return previous user if uid is equal */
		if (previousUser != null && previousUser.getUid() == uid) return previousUser;
		
		VgiAnalysisUser user = userAnalysis.get(uid);
		if (user == null) {
			/** create new user */
			user = new VgiAnalysisUser(uid, username);
			userAnalysis.put(uid, user);
		}
		
		previousUser = user;
		
		return user;
	}
	
	public static void resetParent() {
		VgiAnalysisParent.userAnalysis.clear();
		VgiAnalysisParent.previousUser = null;
//		durationMs = 0;
	}
	
	public void addToProcessingTime(long time) {
		processingTime += time;
	}
	
	public long getProcessingTime() {
		return processingTime;
	}
}
