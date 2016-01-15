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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi;

import java.util.Date;

import com.vividsolutions.jts.geom.Coordinate;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiActionImpl.ActionType;

/**
 * Interface for a factory creating vgi quality analysis object model
 *
 */
public interface IVgiModelFactory {
	
	/**
	 * create a new implementation of IVgiOperation object
	 * 
	 * @return new created IVgiOperation
	 */
	IVgiOperation newOperation(long oid, VgiGeometryType geometryType, VgiOperationType type, int uid, String user, Date timestamp, short version, int changesetid, Coordinate coordinate, String key, String value, long ref, int position);

	/**
	 * create a new implementation of IVgiAction object
	 * 
	 * @return new created IVgiAction
	 */
	IVgiAction newAction(String actionName, ActionType actionType);
	
	/**
	 * create a new implementation of IVgiActivity object
	 * 
	 * @return new created IVgiActivity
	 */
	IVgiActivity newActivity();
}
