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

import java.util.Comparator;

public class VgiActionDefinitionRule {
	private VgiOperationType vgiOperationType = VgiOperationType.OP_UNDEFINED;
	private EntryPointType isEntryPoint = EntryPointType.NO;
	
	public VgiActionDefinitionRule(VgiOperationType operationType, EntryPointType isEntryPoint) {
		this.setVgiOperationType(operationType);
		this.setEntryPoint(isEntryPoint);
	}
	
	public VgiActionDefinitionRule(VgiOperationType operationType) {
		this.setVgiOperationType(operationType);
	}

	public VgiOperationType getVgiOperationType() {
		return vgiOperationType;
	}

	public void setVgiOperationType(VgiOperationType vgiOperationType) {
		this.vgiOperationType = vgiOperationType;
	}

	public EntryPointType getEntryPoint() {
		return isEntryPoint;
	}
	public void setEntryPoint(EntryPointType isEntryPoint) {
		this.isEntryPoint = isEntryPoint;
	}
	
	public enum EntryPointType {
		YES,
		NO,
		IF_FEATURE_TYPE_ADDED,
		IF_FEATURE_TYPE_REMOVED
	}
	
    public static Comparator<VgiActionDefinitionRule> getOperationTypeComparator() {
        return new Comparator<VgiActionDefinitionRule>() {
        	/**
        	 * Compares two VGI operations by operation type hierarchy level
        	 * @param o1
        	 * @param o2
        	 * @return
        	 */
    		public int compare(VgiActionDefinitionRule o1, VgiActionDefinitionRule o2) {
    			return VgiOperationType.getVgiOperationTypeComparator().compare(o1.vgiOperationType, o2.vgiOperationType);
//    			if (o1.getOperationType().getHierarchyLevel() == o2.getOperationType().getHierarchyLevel()) {
//    				return 0;
//    			} else if (o1.getOperationType().getHierarchyLevel() < o2.getOperationType().getHierarchyLevel()) {
//    	            return -1;
//    			} else {
//    	            return 1;
//    			}
    		}
        };
    }
}
