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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.vividsolutions.jts.geom.Geometry;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiActionGenerator;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiFeatureImpl;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiGeometryType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipeline;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.consumer.IVgiPipelineConsumer;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisAction;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisFeature;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl.VgiAnalysisParent;
import gnu.trove.list.array.TLongArrayList;

public class VgiAnalysisConsumer implements IVgiPipelineConsumer, ApplicationContextAware {
	private static Logger log = Logger.getLogger(VgiAnalysisConsumer.class);
	
	private ApplicationContext ctx = null;
	
	private IVgiPipelineSettings settings = null;
	
	private IVgiActionGenerator actionGenerator = null;
	
	private FeatureBuilderConsumer geometryAssemblerConsumer;
	
	private List<IVgiFeature> featureList = null;
	
	private File resultDir = null;
	
	private DecimalFormat df = new DecimalFormat("00");
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private Calendar calA = Calendar.getInstance();
	
	private Map<SimpleFeatureType, DefaultFeatureCollection> mapFeatures = null;
	
	/** Constructor */
	public VgiAnalysisConsumer() {
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		calA.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	
	@Override
	public void doBeforeFirstBatch() {
		/** Write analysis result */
		resultDir = new File(settings.getResultFolder() + File.separator + settings.getFilterPolygonLabel() + File.separator);
		resultDir.mkdir();
		
		/** Initialize variables */
		if (settings.isWriteGeometryFiles()) {
			mapFeatures = new HashMap<SimpleFeatureType, DefaultFeatureCollection>();
		}

		/** Write and reset action analysis */
		for (IVgiAnalysisAction analysis : settings.getActionAnalyzerList()) {
			analysis.reset();
		}
		/** Write and reset operation analysis */
		for (IVgiAnalysisOperation analysis : settings.getOperationAnalyzerList()) {
			analysis.reset();
		}
		/** Write and reset feature analysis */
		for (IVgiAnalysisFeature analysis : settings.getFeatureAnalyzerList()) {
			analysis.reset();
		}
		
		featureList = new ArrayList<IVgiFeature>();
	}
	
	@Override
	public void handleBatch(List<IVgiFeature> batch) {
		prepareOperations(batch);
	}
	
	@Override
	public void doAfterLastBatch() {
		
		/** Write and reset action analysis */
		for (IVgiAnalysisAction analysis : settings.getActionAnalyzerList()) {
			analysis.write(resultDir);
			analysis.reset();
		}
		/** Write and reset operation analysis */
		for (IVgiAnalysisOperation analysis : settings.getOperationAnalyzerList()) {
			analysis.write(resultDir);
			analysis.reset();
		}
		/** Write and reset feature analysis */
		for (IVgiAnalysisFeature analysis : settings.getFeatureAnalyzerList()) {
			analysis.write(resultDir);
			analysis.reset();
		}
		
		/** Reset user analysis */
		VgiAnalysisParent.resetParent();

		if (settings.isWriteGeometryFiles()) {
			
			try {
				for (SimpleFeatureType featureType : mapFeatures.keySet()) {
					write(mapFeatures.get(featureType), new FileOutputStream(resultDir + "/geom_" + featureType.getTypeName() + ".json", false));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
    protected void write(DefaultFeatureCollection features, OutputStream out) throws IOException {
        final FeatureJSON json = new FeatureJSON(new GeometryJSON(7));
        boolean geometryless = features.getSchema().getGeometryDescriptor() == null;
        json.setEncodeFeatureCollectionBounds(!geometryless);
        json.setEncodeFeatureCollectionCRS(!geometryless);
        json.writeFeatureCollection(features, out);
    }
	
	/**
	 * Filters operations, aggregates operations to actions and analyzes all of them
	 */
	private void prepareOperations(List<IVgiFeature> batch) {
		
		/** Filter feature; generate and process actions */
		log.info("Prepare " + batch.size() + " features");
		
		for (IVgiFeature feature : batch) {
			
			/** Filter by tag */
			if (!filterByTag(feature)) continue;
			
			if (settings.getFilterPolygon() != null || settings.isWriteGeometryFiles()) {
				if (settings.getFilterPolygon() != null) {
					if (!settings.getFilterPolygon().getEnvelopeInternal().intersects(feature.getBBox())) continue;
				}
				
				SimpleFeature f = geometryAssemblerConsumer.assembleGeometry(feature, null);
				if (f == null) continue;
				
				if (settings.getFilterPolygon() != null) {
					Geometry geometry = (Geometry)f.getDefaultGeometry();
					if (geometry == null || geometry.disjoint(settings.getFilterPolygon())) continue;					
				}
				
				if (settings.isWriteGeometryFiles()) {
					if (!mapFeatures.containsKey(f.getFeatureType())) mapFeatures.put(f.getFeatureType(), new DefaultFeatureCollection(f.getFeatureType().getTypeName(), f.getFeatureType()));
					mapFeatures.get(f.getFeatureType()).add(f);
				}
			}
			
			featureList.add(feature);
		}
		
		analyzeFeatures();
	}
	
	private void analyzeFeatures() {
		
		if (settings.isFindRelatedOperations()) {
			log.info("Find related operations for " + featureList.size() + " Features");
			findRelatedOperations();
		}

		log.info("Analyze " + featureList.size() + " Features");
		for (IVgiFeature feature : this.featureList) {
			analyzeFeature(feature);
		}
		
		/** Clear feature list */
		featureList.clear();
	}
	
	private void findRelatedOperations() {
		
		/** Create relatedFeatureList which includes nodes which have been added to ways */
		Map<Long, List<IVgiOperation>> childElementList = new HashMap<Long, List<IVgiOperation>>();
		TLongArrayList featureIdList = new TLongArrayList();
		for (IVgiFeature feature : featureList) {
			if (!feature.getVgiGeometryType().equals(VgiGeometryType.LINE)) continue;
			featureIdList.add(feature.getOid());
			for (IVgiOperation operation : feature.getOperationList()) {
				if (!operation.getVgiOperationType().equals(VgiOperationType.OP_ADD_NODE)) continue;
				if (!childElementList.containsKey(operation.getRefId())) {
					childElementList.put(operation.getRefId(), null);
				}
			}
		}
		
		/** Return if no child elements have been found */
		if (childElementList.size() == 0) return;

		/** Find related features */
		IVgiPipeline pipeline = ctx.getBean("vgiRelatedFeaturePipeline", IVgiPipeline.class);
		
		RelatedVgiOperationFinderConsumer consumer = ctx.getBean("relatedVgiOperationFinderConsumer", RelatedVgiOperationFinderConsumer.class);
		consumer.setChildElementList(childElementList);
		consumer.setFeatureList(featureList);
		
		pipeline.setFilterNodeId(new TLongArrayList());
		pipeline.setFilterWayId(featureIdList);
		pipeline.setFilterRelationId(new TLongArrayList());
		pipeline.start();
	}
	
	/**
	 * Sends feature to operation/action/feature analyzer
	 * @param feature
	 */
	private void analyzeFeature(IVgiFeature feature) {
		/** Analyze operations */
		if (settings.getOperationAnalyzerList().size() > 0) {
			for (IVgiOperation operation : feature.getOperationList()) {
				for (IVgiAnalysisOperation analysis : settings.getOperationAnalyzerList()) {
					analysis.analyze(operation, deriveTimePeriod(operation)[0]);
				}
			}
		}
		
		/** Analyze actions */
		if (settings.getActionAnalyzerList().size() > 0) {
			if (feature.getActionList() == null) {
				actionGenerator.generateActions(feature);
			}
			if (feature.getActionList().size() == 0) return;
			
			for (IVgiAction action : feature.getActionList()) {
				
				if (action.getOperations().size() == 0) continue;
				
				IVgiOperation firstOperation = action.getOperations().get(0);
				
				/** Determine/derive time period */
				if (firstOperation.getTimestamp().before(settings.getAnalysisStartDate())) continue;
				if (firstOperation.getTimestamp().after(settings.getAnalysisEndDate())) continue;
				
				/** Analyze action */
				for (IVgiAnalysisAction analysis : settings.getActionAnalyzerList()) {
					analysis.analyze(action, deriveTimePeriod(firstOperation)[0]);
				}
			}
		}
		
		/** Analyze feature */
		if (settings.getFeatureAnalyzerList().size() > 0) {
			IVgiOperation firstOperation = feature.getActionList().get(0).getOperations().get(0);
			
			if (firstOperation.getTimestamp().before(settings.getAnalysisStartDate())) return;
			if (firstOperation.getTimestamp().after(settings.getAnalysisEndDate())) return;
			
			for (IVgiAnalysisFeature analysis : settings.getFeatureAnalyzerList()) {
				analysis.analyze(feature, null);
			}
		}
		
		/** Release feature */
		feature  = null;
	}
	
	/**
	 * Derives the time period from the operation timestamp
	 * @param operation operation with a timestamp
	 * @return time period
	 */
	private Date[] deriveTimePeriod(IVgiOperation operation) {
		Date[] timePeriod = new Date[2];
		
		/** Create new time period (year, month, day); no timestamp filter */
		calA.setTime(operation.getTimestamp());
	    
		try {
			if (settings.getTemporalResolution().equals("day")) {
				/** DAY */
				timePeriod[0] = dateFormat.parse(""+calA.get(Calendar.YEAR)+"-"+df.format(calA.get(Calendar.MONTH)+1)+"-"+calA.get(Calendar.DAY_OF_MONTH)+"");
//				calA.add(Calendar.DAY_OF_MONTH, 1);
//				timePeriod[1] = dateFormat.parse(""+calA.get(Calendar.YEAR)+"-"+df.format(calA.get(Calendar.MONTH)+1)+"-"+calA.get(Calendar.DAY_OF_MONTH)+"");
				
			} else if (settings.getTemporalResolution().equals("month")) {
				/** MONTH */
				timePeriod[0] = dateFormat.parse(""+calA.get(Calendar.YEAR)+"-"+df.format(calA.get(Calendar.MONTH)+1)+"-01");
//				calA.add(Calendar.MONTH, 1);
//				timePeriod[1] = dateFormat.parse(""+calA.get(Calendar.YEAR)+"-"+df.format(calA.get(Calendar.MONTH)+1)+"-01");

			} else if (settings.getTemporalResolution().equals("year")) {
				/** YEAR */
				timePeriod[0] = dateFormat.parse(""+calA.get(Calendar.YEAR)+"-01-01");
//				calA.add(Calendar.YEAR, 1);
//				timePeriod[1] = dateFormat.parse(""+calA.get(Calendar.YEAR)+"-01-01");
				
			} else if (settings.getTemporalResolution().equals("decade")) {
				/** DECATE */
				timePeriod[0] = dateFormat.parse(""+((int)((double)calA.get(Calendar.YEAR)/10))*10+"-01-01");
				
			} else if (settings.getTemporalResolution().equals("century")) {
				/** CENTURY */
				timePeriod[0] = dateFormat.parse(""+((int)((double)calA.get(Calendar.YEAR)/100))*100+"-01-01");
				
			} else {
				/** DEFAULT */
				timePeriod[0] = settings.getAnalysisStartDate();
			}
		} catch (ParseException e) {
			log.error("Cannot parse date");
		}
		
		return timePeriod;
	}
	
	/**
	 * Retrieves tags which exist at the time of <code>timestamp</code> and applies feature type filter
	 * @param feature
	 * @return true if feature contains tag mentioned in tag filter
	 */
	private boolean filterByTag(IVgiFeature feature) {
		/** if no tag is defined, return true */
		if (settings.getFilterTag().keySet().size() == 0) return true;
		
		/** Filter feature by tag */
		Map<String, List<String>> tags = VgiFeatureImpl.getAllTagsFromOperations(feature);
		
		for (String tagKey : tags.keySet()) {
			
			/** Does filter list contain the key of this operation? */
			if (settings.getFilterTag().containsKey(tagKey)) {
				
				/** Return true if filter list contains the value of this operation */
				/** Compare only tag keys */
				if (settings.getFilterTag().get(tagKey).size() == 0) return true;
				
				/** Compare keys and values */
				tags.get(tagKey).retainAll(settings.getFilterTag().get(tagKey));
				if (tags.get(tagKey).size() > 0) return true;
			}
		}
		/** Otherwise return false */
		return false;
	}

	@Override
	public void setApplicationContext(ApplicationContext ctx) throws BeansException {
		this.ctx = ctx;
	}
	
	public void setSettings(IVgiPipelineSettings settings) {
		this.settings = settings;
	}
	
	public void setActionGenerator(IVgiActionGenerator actionGenerator) {
		this.actionGenerator = actionGenerator;
	}
	
	public void setGeometryAssemblerConsumer(FeatureBuilderConsumer geometryAssemblerConsumer) {
		this.geometryAssemblerConsumer = geometryAssemblerConsumer;
	}
}
