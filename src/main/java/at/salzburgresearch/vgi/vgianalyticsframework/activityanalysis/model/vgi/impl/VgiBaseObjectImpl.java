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

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiBaseObject;

public abstract class VgiBaseObjectImpl implements IVgiBaseObject {

	private long oid = -1; //object id
	private VgiGeometryType vgiGeometryType;
	private short version = -1;
	private int uid = -1;
	private String user = null;
	private Date timestamp = null;
	
	public VgiBaseObjectImpl() {}
	
	@Override
	public long getOid() {
		return oid;
	}
	@Override
	public void setOid(long oid) {
		this.oid = oid;
	}
	
	@Override
	public VgiGeometryType getVgiGeometryType() {
		return vgiGeometryType;
	}
	@Override
	public void setVgiGeometryType(VgiGeometryType vgiGeometryType) {
		this.vgiGeometryType = vgiGeometryType;
	}
	
	@Override
	public short getVersion() {
		return version;
	}
	@Override
	public void setVersion(short version) {
		this.version = version;
	}
	
	@Override
	public int getUid() {
		return uid;
	}
	@Override
	public void setUid(int uid) {
		this.uid = uid;
	}

	@Override
	public String getUser() {
		return user;
	}
	@Override
	public void setUser(String user) {
		this.user = user;
	}

	@Override
	public Date getTimestamp() {
		return timestamp;
	}
	@Override
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}
}
