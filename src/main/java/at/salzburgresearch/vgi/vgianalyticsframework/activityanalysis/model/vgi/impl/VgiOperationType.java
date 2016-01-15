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

public enum VgiOperationType {
	OP_RECREATE_NODE(0, 40),
	OP_RECREATE_WAY(1, 40),
	OP_RECREATE_RELATION(2, 40),
	OP_REPLACE_NODE(3, 39),
	OP_REPLACE_WAY(4, 39),
	OP_REPLACE_RELATION(5, 39),
	OP_SPLIT_WAY(6, 35),
	OP_MERGE_WAY(7, 34),
	OP_CREATE_NODE(8, 30),
	OP_CREATE_WAY(9, 30),
	OP_CREATE_RELATION(10, 30),
	OP_DELETE_NODE(11, 25),
	OP_DELETE_WAY(12, 25),
	OP_DELETE_RELATION(13, 25),
	OP_ADD_TAG(18, 12),
	OP_MODIFY_TAG_VALUE(19, 11),
	OP_REMOVE_TAG(20, 10),
	OP_REVERSE_WAY(21, 9),
	OP_REMOVE_NODE(23, 8),
	OP_ADD_NODE(22, 7),
	OP_REMOVE_MEMBER(25, 5),
	OP_ADD_MEMBER(24, 4),
	OP_REORDER_NODE(36, 3),
	OP_REORDER_MEMBER(37, 3),
	OP_MODIFY_COORDINATE(26, 2),
	OP_MODIFY_WAY_COORDINATE(27, 2),
	OP_MODIFY_ROLE(28, 1),
	OP_UNDEFINED(29, 0);

    private int id;
    private int hierarchyLevel;

    private VgiOperationType(int id, int hierarchyLevel) {
    	this.id = id;
        this.hierarchyLevel = hierarchyLevel;
    }
    
    public int getId() {
    	return this.id;
    }
    
    public int getHierarchyLevel() {
        return this.hierarchyLevel;
    }
    
    public static VgiOperationType getOperationTypeById(int id) {
    	for (VgiOperationType opType : VgiOperationType.values()) {
    		if (opType.getId() == id) return opType;
    	}
    	return VgiOperationType.OP_UNDEFINED;
    }
    
    public static Comparator<VgiOperationType> getVgiOperationTypeComparator() {
        return new Comparator<VgiOperationType>() {
        	/**
        	 * Compares two VGI operations by operation type hierarchy level
        	 * @param o1
        	 * @param o2
        	 * @return
        	 */
    		public int compare(VgiOperationType o1, VgiOperationType o2) {
    			if (o1.getHierarchyLevel() == o2.getHierarchyLevel()) {
    				return 0;
    			} else if (o1.getHierarchyLevel() < o2.getHierarchyLevel()) {
    	            return -1;
    			} else {
    	            return 1;
    			}
    		}
        };
    }
}

