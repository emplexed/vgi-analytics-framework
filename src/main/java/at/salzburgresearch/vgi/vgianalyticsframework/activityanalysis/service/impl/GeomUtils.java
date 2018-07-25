package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;

public class GeomUtils {

	// source http://de.wikipedia.org/wiki/World_Geodetic_System_1984
	private static double wgs84_a = 6378137.000;

	private static double wgs84_b = 6356752.314;
	
	private static double wgs84_f = (wgs84_a - wgs84_b) / wgs84_a;
	
	
	/**
	 * Calculates the Length of a linestring in meters with the method from Andoyer
	 * @param geometry
	 * @return
	 */
	public static double calculateLengthMeterFromWGS84LineStringAndoyer(LineString geometry) {
		
//		if (geometry.getSRID() != WGS84) {
//			throw new IllegalArgumentException("parameter geometry is not a WGS84 linestring!");
//		}
		
		Coordinate coords[] = geometry.getCoordinates();
		double length = 0;
		for (int i = 1; i < coords.length; i++) {
			length += distanceAndoyer(coords[i - 1], coords[i]);
		}
		return length;
	}
	
	 /**
	  * nach https://de.wikibooks.org/wiki/Astronomische_Berechnungen_f%C3%BCr_Amateure/_Distanzen/_Erdglobus#Abstand_zweier_Punkte_auf_der_Erdoberfl.C3.A4che
	  * siehe auch https://de.wikipedia.org/wiki/Henri_Andoyer
	  * 
	  * NOTE Coordinates are assumed to be in WGS84 Format and not checked.
	  * 
	  * @param start
	  * @param end
	  * @return
	  */
	public static double distanceAndoyer(Coordinate start, Coordinate end) {

     double lambdaA = Math.toRadians(start.x);
     double phiA = Math.toRadians(start.y);
     double lambdaB = Math.toRadians(end.x);
     double phiB = Math.toRadians(end.y);
     
     double F = (phiA + phiB) / 2;
     double G = (phiA - phiB) / 2;
     double L = (lambdaA - lambdaB) / 2;
     
     if (G == 0 && L == 0) return 0;
     
     double squSinL = Math.pow(Math.sin(L), 2);
     double squCosL = Math.pow(Math.cos(L), 2);        
     double squSinG = Math.pow(Math.sin(G), 2);
     double squCosG =  Math.pow(Math.cos(G), 2);
     double squSinF = Math.pow(Math.sin(F), 2);
     double squCosF =  Math.pow(Math.cos(F), 2);
     
     double S = squSinG * squCosL + squCosF * squSinL;
     double C = squCosG * squCosL + squSinF * squSinL;
     
     double w = Math.atan(Math.sqrt(S / C));
     
     double R = Math.sqrt(S * C) / w;
     
     double D = 2 * w * wgs84_a;
     
     double H1 = (3 * R - 1) / (2 * C);
     double H2 = (3 * R + 1) / (2 * S);
  
     
     double d = D * (1 + wgs84_f * H1 * squSinF * squCosG - wgs84_f * H2 * squCosF * squSinG);
     
     if (Double.isNaN(d)) {
         throw new RuntimeException("GeometryUtils.distanceAndoyer() result is not a number");
     }
     return d;
 }
}
