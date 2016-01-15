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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl;

import java.util.Date;

import com.vividsolutions.jts.geom.Coordinate;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiActivity;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiModelFactory;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiActionImpl.ActionType;

public class VgiModelFactoryImpl implements IVgiModelFactory {

	@Override
	public IVgiOperation newOperation(long oid, VgiGeometryType geometryType, VgiOperationType type, int uid, String user, Date timestamp, short version, int changesetid, Coordinate coordinate, String key, String value, long ref, int position) {
		return new VgiOperationImpl(oid, geometryType, type, uid, user, timestamp, version, changesetid, coordinate, key, value, ref, position);
	}
	
	@Override
	public IVgiAction newAction(String actionName, ActionType actionType) {
		return new VgiActionImpl(actionName, actionType);
	}

	@Override
	public IVgiActivity newActivity() {
		return new VgiActivityImpl();
	}

}
