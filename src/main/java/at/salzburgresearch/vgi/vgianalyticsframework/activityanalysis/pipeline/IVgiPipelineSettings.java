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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Map;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeatureType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiPolygon;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisOperation;

public interface IVgiPipelineSettings {

	/**
	 * Reads VGI settings from a XML file
	 * @return 
	 */
	boolean loadSettings(File file);
	
	String getSettingName();
	void setSettingName(String name);
	
	File getPbfDataFolder();
	void setPbfDataFolder(File pbfDataFolder);
	
	boolean isReadQuadtree();
	void setReadQuadtree(boolean readQuadtree);
	
	File getResultFolder();
	
	boolean isIgnoreFeaturesWithoutTags();
	boolean isFindRelatedOperations();
	boolean isWriteGeometryFiles();
	boolean isIgnoreNonFeatureTypeTags();
	
	Date getFilterTimestamp();
	long getFilterUid();
	VgiGeometryType getFilterElementType();
	Map<String, List<String>> getFilterTag();

	List<VgiPolygon> getFilterPolygonList();
	void setFilterPolygonList(List<VgiPolygon> filterPolygonList);
	VgiPolygon getCurrentPolygon();
	void setCurrentPolygon(VgiPolygon polygon);

	Date getAnalysisStartDate();
	Date getAnalysisEndDate();
	String getTemporalResolution();
	
	long getActionTimeBuffer();
	List<IVgiAction> getActionDefinitionList();
	
	List<IVgiAnalysisAction> getActionAnalyzerList();
	List<IVgiAnalysisOperation> getOperationAnalyzerList();
	List<IVgiAnalysisFeature> getFeatureAnalyzerList();
	
	Map<String, IVgiFeatureType> getFeatureTypeList();
	
	Map<String, List<IVgiFeature>> getCache();
	void setCache(Map<String, List<IVgiFeature>> cache);

	int getKeepInCacheLevel();
	void setKeepInCacheLevel(int keepInCacheLevel);
}
