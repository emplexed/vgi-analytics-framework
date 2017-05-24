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

package at.salzburgresearch.vgi.vgianalyticsframework.osm.importer.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.openstreetmap.osmosis.osmbinary.BinaryParser;
import org.openstreetmap.osmosis.osmbinary.Osmformat;
import org.openstreetmap.osmosis.osmbinary.Osmformat.DenseInfo;

import com.vividsolutions.jts.geom.Coordinate;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Node;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.OsmElementType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Relation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.RelationMember;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.osm.impl.Way;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.importer.OsmDataConsumer;

/**
 * Parses OSM PBF files and produces OSM elements<br />
 * Extends Importer by Osmosis
 */
public class OsmPbfParser extends BinaryParser {
	private static Logger log = org.apache.logging.log4j.LogManager.getLogger(VgiOperationImpl.class);

    private OsmDataConsumer osmDataConsumer = null;

    @Override
    public void complete() {
    	osmDataConsumer.afterProcessing();
    }
    
	@Override
	protected void parseNodes(List<Osmformat.Node> pbfNodes) {
		for (Osmformat.Node pbfNode : pbfNodes) {
			Map<String, String> tags = new HashMap<String, String>();
			for (int j = 0; j < pbfNode.getKeysCount(); j++) {
				tags.put(getStringById(pbfNode.getKeys(j)), getStringById(pbfNode.getVals(j)));
			}
			Node osmNode = null;
			long id = pbfNode.getId();
			double latf = parseLat(pbfNode.getLat()), lonf = parseLon(pbfNode.getLon());

      	boolean visible = true;
      	
			if (pbfNode.hasInfo()) {
				Osmformat.Info info = pbfNode.getInfo();
				if (info.hasVisible()) visible = info.getVisible();
				osmNode = new Node(OsmElementType.NODE, id, info.getUid(), getStringById(info.getUserSid()),
						getDate(info), (int) info.getChangeset(), (short) info.getVersion(), visible, tags,
						(info.getVisible() ? new Coordinate(lonf, latf) : null));
			}
			osmDataConsumer.process(osmNode);
		}
	}
    
	@Override
	protected void parseDense(Osmformat.DenseNodes pbfNodes) {
		long lastId = 0, lastLat = 0, lastLon = 0;

		int j = 0; // Index into the keysvals array.

		// Stuff for dense info
		long lasttimestamp = 0;
		int lastusernameId = 0, lastuid = 0, lastchangeset = 0;
		DenseInfo di = null;
		if (pbfNodes.hasDenseinfo()) {
			di = pbfNodes.getDenseinfo();
		}
		for (int i = 0; i < pbfNodes.getIdCount(); i++) {
			Node osmNode = null;
			long lat = pbfNodes.getLat(i) + lastLat;
			lastLat = lat;
			long lon = pbfNodes.getLon(i) + lastLon;
			lastLon = lon;
			long id = pbfNodes.getId(i) + lastId;
			lastId = id;
			double latf = parseLat(lat), lonf = parseLon(lon);
			Map<String, String> tags = new HashMap<String, String>();
			// If empty, assume that nothing here has keys or vals.
			if (pbfNodes.getKeysValsCount() > 0) {
				while (pbfNodes.getKeysVals(j) != 0) {
					int keyid = pbfNodes.getKeysVals(j++);
					int valid = pbfNodes.getKeysVals(j++);
					tags.put(getStringById(keyid), getStringById(valid));
				}
				j++; // Skip over the '0' delimiter.
			}
			// Handle dense info.
			if (di != null) {
				int uid = di.getUid(i) + lastuid;
				lastuid = uid;
				int usernameId = di.getUserSid(i) + lastusernameId;
				lastusernameId = usernameId;
				long timestamp = di.getTimestamp(i) + lasttimestamp;
				lasttimestamp = timestamp;
				short version = (short) di.getVersion(i);
				int changeset = (int) di.getChangeset(i) + lastchangeset;
				lastchangeset = changeset;
				boolean visible = di.getVisible(i);

				Date date = new Date(date_granularity * timestamp);

				osmNode = new Node(OsmElementType.NODE, id, uid, getStringById(usernameId), date, changeset, version,
						visible, tags, (visible ? new Coordinate(lonf, latf) : null));
			}
			osmDataConsumer.process(osmNode);
		}
	}
    
    @Override
    protected void parseWays(List<Osmformat.Way> pbfWays) {
        for (Osmformat.Way pbfWay : pbfWays) {
        	Map<String, String> tags = new HashMap<String, String>();
            for (int j = 0; j < pbfWay.getKeysCount(); j++) {
                tags.put(getStringById(pbfWay.getKeys(j)), getStringById(pbfWay.getVals(j)));
            }
            
            long lastId = 0;
            List<Long> nodes = new ArrayList<Long>();
            for (long j : pbfWay.getRefsList()) {
                nodes.add(j + lastId);
                lastId = j + lastId;
            }
            
            boolean visible = true;
            
            Way osmWay = null;
            if (pbfWay.hasInfo()) {
                Osmformat.Info info = pbfWay.getInfo();
                if (info.hasVisible()) visible = info.getVisible();
                
                osmWay = new Way(OsmElementType.WAY, pbfWay.getId(), info.getUid(), getStringById(info.getUserSid()), getDate(info), (int) info.getChangeset(), (short) info.getVersion(), visible, tags, nodes);
            }
            osmDataConsumer.process(osmWay);
        }
    }

    @Override
    protected void parseRelations(List<Osmformat.Relation> pbfRelations) {
        for (Osmformat.Relation pbfRelation : pbfRelations) {
        	Map<String, String> tags = new HashMap<String, String>();
            for (int j = 0; j < pbfRelation.getKeysCount(); j++) {
                tags.put(getStringById(pbfRelation.getKeys(j)), getStringById(pbfRelation.getVals(j)));
            }
            
            long lastMemberId = 0;
            List<RelationMember> nodes = new ArrayList<RelationMember>();
            for (int j = 0; j < pbfRelation.getMemidsCount(); j++) {
                long memberId = lastMemberId + pbfRelation.getMemids(j);
                lastMemberId = memberId;
                OsmElementType elementType = OsmElementType.UNKOWN;

                if (pbfRelation.getTypes(j) == Osmformat.Relation.MemberType.NODE) {
                    elementType = OsmElementType.NODE;
                } else if (pbfRelation.getTypes(j) == Osmformat.Relation.MemberType.WAY) {
                    elementType = OsmElementType.WAY;
                } else if (pbfRelation.getTypes(j) == Osmformat.Relation.MemberType.RELATION) {
                    elementType = OsmElementType.RELATION;
                }
                
                nodes.add(new RelationMember(memberId, elementType, getStringById(pbfRelation.getRolesSid(j))));
            }

            boolean visible = true;
            
            Relation osmRelation = null;
            if (pbfRelation.hasInfo()) {
                Osmformat.Info info = pbfRelation.getInfo();
                if (info.hasVisible()) visible = info.getVisible();
                
                osmRelation = new Relation(OsmElementType.RELATION, pbfRelation.getId(), info.getUid(), getStringById(info.getUserSid()), getDate(info), (int) info.getChangeset(), (short) info.getVersion(), visible, tags, nodes);
            }
            osmDataConsumer.process(osmRelation);
        }
    }

	@Override
	public void parse(Osmformat.HeaderBlock block) {
		for (String s : block.getRequiredFeaturesList()) {
			if (s.equals("OsmSchema-V0.6")) {
				continue;
			}
			if (s.equals("DenseNodes")) {
				continue;
			}
			if (s.equals("HistoricalInformation")) {
				continue;
			}
			log.error("Cannot parse file");
			System.exit(0);
		}
	}
    
    public void setSink(OsmDataConsumer osmDataConsumer) {
       this.osmDataConsumer = osmDataConsumer;
    }
}