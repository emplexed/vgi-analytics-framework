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

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiUser;

public class VgiContributorImpl implements IVgiUser {
	private int uid = 0;
	private String username = "";
	
	public VgiContributorImpl() {}
	
	public VgiContributorImpl(int uid, String username) {
		this.uid = uid;
		this.username = username;
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
	public String getUsername() {
		return username;
	}
	@Override
	public void setUsername(String username) {
		this.username = username;
	}
}
