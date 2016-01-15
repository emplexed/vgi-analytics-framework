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
import java.util.Date;

import com.vividsolutions.jts.geom.Coordinate;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;

public class VgiOperationImpl extends VgiBaseObjectImpl implements IVgiOperation {
//	private static Logger log = Logger.getLogger(VgiOperationImpl.class);
	
	private static final int magicMultiplier = 10000000;
	
	private VgiOperationType operationType = VgiOperationType.OP_UNDEFINED;
	
	private int changesetid = -1;
	private Coordinate coordinate = null;
	private String key = "";
	private String value = "";
	private long refId = -1;
	private int position = -1;
	
	public VgiOperationImpl () {
		this.setVgiOperationType(VgiOperationType.OP_UNDEFINED);
		this.setVgiGeometryType(VgiGeometryType.UNDEFINED);
	};
	
	public VgiOperationImpl (VgiOperationType type) {
		this.setVgiOperationType(type);
	}
	
	public VgiOperationImpl (long oid, VgiGeometryType vgiGeometryType, VgiOperationType type, int uid, String user, Date timestamp, short version, int changesetid, Coordinate coordinate, String key, String value, long ref, int position) {
		this.setOid(oid);
		this.setVgiGeometryType(vgiGeometryType);
		this.setTimestamp(timestamp);
		this.setUid(uid);
//		this.setUser(user);
		this.setVersion(version);
		this.setVgiOperationType(type);
		this.setChangesetid(changesetid);
		
		this.setCoordinate(coordinate);
		this.setKey(key);
		this.setValue(value);
		this.setRefId(ref);
		this.setPosition(position);
	}
	
	public VgiOperationImpl(IVgiOperation op) {
		this(op.getOid(), op.getVgiGeometryType(), op.getVgiOperationType(), op.getUid(), op.getUser(), op.getTimestamp(), op.getVersion(), op.getChangesetid(), ((op.getCoordinate() != null) ? new Coordinate(op.getCoordinate()) : null), op.getKey(), op.getValue(), op.getRefId(), op.getPosition());
	}
	
	@Override
	public VgiOperationType getVgiOperationType() {
		return operationType;
	}

	@Override
	public void setVgiOperationType(VgiOperationType operationType) {
		this.operationType = operationType;
	}

	@Override
	public int getChangesetid() {
		return changesetid;
	}

	@Override
	public void setChangesetid(int changesetid) {
		this.changesetid = changesetid;
	}

	@Override
	public Coordinate getCoordinate() {
		return coordinate;
	}

	@Override
	public void setCoordinate(Coordinate coordinate) {
		this.coordinate = coordinate;
	}

	@Override
	public void setCoordinateFromInteger(int longitude, int latitude) {
		double[] coord = coordinateToDouble(longitude, latitude);
		this.coordinate = new Coordinate(coord[0], coord[1]);
	}
	
	@Override
	public String getCoordinateWKT() {
		return "POINT(" + coordinate.x + " " + coordinate.y + ")";
	}
	
	@Override
	public int[] getCoordinateAsInteger() {
		return coordinateToInt(this.coordinate.x, this.coordinate.y);
	}

	@Override
	public String getKey() {
		return key;
	}

	@Override
	public void setKey(String key) {
		this.key = key;
	}

	@Override
	public String getValue() {
		return value;
	}

	@Override
	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public long getRefId() {
		return refId;
	}

	@Override
	public void setRefId(long ref) {
		this.refId = ref;
	}

	@Override
	public int getPosition() {
		return position;
	}
	@Override
	public void setPosition(int position) {
		this.position = position;
	}

	@Override
	public String toString() {
		return "Operation " + this.operationType + " (" + this.getVgiGeometryType() + "/" + this.getOid() + ")";
	}
	
	private static int[] coordinateToInt(double x, double y) {
		int[] coordinate = new int[2];
		coordinate[0] = (int)Math.round(Double.valueOf(x)*magicMultiplier);
		coordinate[1] = (int)Math.round(Double.valueOf(y)*magicMultiplier);
		return coordinate;
	}
	
	private static double[] coordinateToDouble(int x, int y) {
		double[] coordinate = new double[2];
		coordinate[0] = (double)x/magicMultiplier;
		coordinate[1] = (double)y/magicMultiplier;
		return coordinate;
	}
	
    public static Comparator<IVgiOperation> getVersionComparator() {
        return new Comparator<IVgiOperation>() {
        	/**
        	 * Compares two VGI operations by version ASC, timestamp ASC, hierarchy level DESC
        	 * @param o1 operation 1
        	 * @param o2 operation 2
        	 * @return comparison value
        	 */
    		public int compare(IVgiOperation o1, IVgiOperation o2) {
    			if (o1.getVersion() == o2.getVersion()) {
    				return getTimestampComparator().compare(o1, o2);
    			} else if (o1.getVersion() < o2.getVersion()) {
    	            return -1;
    			} else {
    	            return 1;
    			}
    		}
        };
    }
	
    public static Comparator<IVgiOperation> getTimestampComparator() {
        return new Comparator<IVgiOperation>() {
        	/**
        	 * Compares two VGI operations by timestamp ASC, hierarchy level DESC
        	 * @param o1 operation 1
        	 * @param o2 operation 2
        	 * @return comparison value
        	 */
    		public int compare(IVgiOperation o1, IVgiOperation o2) {
				if (o1.getTimestamp().equals(o2.getTimestamp())) {
					return VgiOperationType.getVgiOperationTypeComparator().compare(o1.getVgiOperationType(), o2.getVgiOperationType()) * -1;
    			} else if (o1.getTimestamp().before(o2.getTimestamp())) {
    	            return -1;
    			} else {
    	            return 1;
    			}
    		}
        };
    }
	
    public static Comparator<IVgiOperation> getOperationTypeComparator() {
        return new Comparator<IVgiOperation>() {
        	/**
        	 * Compares two VGI operations by operation type hierarchy level
        	 * @param o1
        	 * @param o2
        	 * @return
        	 */
    		public int compare(IVgiOperation o1, IVgiOperation o2) {
    			return VgiOperationType.getVgiOperationTypeComparator().compare(o1.getVgiOperationType(), o2.getVgiOperationType());
    		}
        };
    }
}
