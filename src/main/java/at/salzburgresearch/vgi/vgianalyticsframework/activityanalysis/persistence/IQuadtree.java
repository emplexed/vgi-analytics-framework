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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence;

import java.util.List;

import com.vividsolutions.jts.geom.Envelope;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.impl.QuadtreeImpl;

public interface IQuadtree {
	void insertFeature(IVgiFeature feature);

	List<IVgiFeature> findFeature(Envelope bbox);

	void allocateFeatures(List<IVgiFeature> featureList);
	
	List<IVgiFeature> getFeatureList();
	void setFeatureList(List<IVgiFeature> featureList);

//	QuadtreeImpl getParent();
//	void setParent(QuadtreeImpl parent);

	void setNW(QuadtreeImpl nW);
	QuadtreeImpl getNW();

	void setNE(QuadtreeImpl nE);
	QuadtreeImpl getNE();

	void setSE(QuadtreeImpl sE);
	QuadtreeImpl getSE();

	void setSW(QuadtreeImpl sW);
	QuadtreeImpl getSW();

	void setSplitX(double splitX);
	double getSplitX();

	void setSplitY(double splitY);
	double getSplitY();

	void setDimensionX(double dimensionX);
	double getDimensionX();

	void setDimensionY(double dimensionY);
	double getDimensionY();

	void setPath(String path);
	String getPath();

	void setLevel(int level);
	int getLevel();

	void setFeatureCount(int featureCount);
	int getFeatureCount();
}
