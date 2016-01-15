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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.persistence.impl;

import java.util.EventObject;

public class FeatureImportEvent extends EventObject {
	/** serialVersionUID to avoid InvalidClassException */
	private static final long serialVersionUID = -2282937598325318825L;

	public FeatureImportEvent(Object source) {
		super(source);
		
		
	}
}
