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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.analysis.impl;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.geotools.factory.Hints;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.IVgiOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiOperationType;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.IVgiAnalysisOperation;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl.CSVFileWriter;

public class VgiAnalysisUpdateGeometry extends VgiAnalysisParent implements IVgiAnalysisOperation {
	
	private CoordinateReferenceSystem sourceCRS = null;
	private CoordinateReferenceSystem targetCRS = null;
	private MathTransform transform = null;
	
	private GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
	
	private Map<Short, GeometryUpdates> data = new HashMap<Short, GeometryUpdates>();
	
	/** Constructor */
	public VgiAnalysisUpdateGeometry() {		
		try {
			sourceCRS = CRS.decode("EPSG:4326");
			targetCRS = CRS.decode("EPSG:31258");
			
			CRSAuthorityFactory factory = ReferencingFactoryFinder.getCRSAuthorityFactory("EPSG", new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE));
			sourceCRS = factory.createCoordinateReferenceSystem("EPSG:4326");
			targetCRS = factory.createCoordinateReferenceSystem("EPSG:31258");
			
			transform = CRS.findMathTransform(sourceCRS, targetCRS, true);
		} catch (NoSuchAuthorityCodeException e) {
			e.printStackTrace();
		} catch (FactoryException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void analyze(IVgiOperation operation, Date timePeriod) {

		if (!operation.getVgiOperationType().equals(VgiOperationType.OP_ADD_NODE) && !operation.getVgiOperationType().equals(VgiOperationType.OP_MODIFY_WAY_COORDINATE)) return;
		
		if (!data.containsKey(operation.getVersion())) {
			data.put(operation.getVersion(), new GeometryUpdates());
		}
		GeometryUpdates geomUpdates = data.get(operation.getVersion());
		
		Point currentCoordinate = null;
		
		try {
//			transform = CRS.findMathTransform(sourceCRS, targetCRS, true);
			
			Point point = geometryFactory.createPoint(operation.getCoordinate());
			point.setSRID(4326);

//			MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS);
			currentCoordinate = (Point)JTS.transform( point, transform);
//		} catch (FactoryException e) {
//			e.printStackTrace();
		} catch (TransformException e) {
			e.printStackTrace();
		}
		
		if (geomUpdates.coordinates.containsKey(operation.getRefId())) {
			geomUpdates.updatesCount++;
			geomUpdates.sumCoordinateDelta += geomUpdates.coordinates.get(operation.getRefId()).distance(currentCoordinate);
			
		}
		
		geomUpdates.coordinates.put(operation.getRefId(), currentCoordinate);
		
	}
	
	@Override
	public void write(File path) {
		
		try (CSVFileWriter writer = new CSVFileWriter(path + "/update_geometry.csv")) {
				
			writer.writeLine("wayId;nodeCount;updatesCount;sumCoordinateDelta");
			
			for (Short key : data.keySet()) {
				GeometryUpdates g = data.get(key);
				writer.writeLine(key + ";" + g.coordinates.size() + ";" + g.updatesCount + ";" + decimalFormat.format(g.sumCoordinateDelta));
		//			writer.writeLine(dateFormatYear.format(key) + ";" + g.coordinates.size() + ";" + g.updatesCount + ";" + decimalFormat.format(g.sumCoordinateDelta));
			}
		} catch (IOException e) {
			log.error("Error while writing CSV file", e);
		}
	}
	
	@Override
	public void reset() {
		data = new HashMap<Short, GeometryUpdates>();
	}
	
	@Override
	public String toString() {
		return "VgiAnalysisUserPerOperation";
	}
	
	private class GeometryUpdates {
		private int updatesCount;
		private double sumCoordinateDelta;
		
		/** coordinate store */
		private Map<Long, Point> coordinates = new HashMap<Long, Point>(); //nodeId, coordinate
		
	}
}
