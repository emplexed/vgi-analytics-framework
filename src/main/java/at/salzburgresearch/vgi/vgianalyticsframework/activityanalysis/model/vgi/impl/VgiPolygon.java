package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl;

import com.vividsolutions.jts.geom.Polygon;

/**
 * Polygon with label used in VGI settings
 *
 */
public class VgiPolygon {
	private Polygon polygon = null;
	private String label = null;
	
	public VgiPolygon() {
		
	}
	
	public VgiPolygon(Polygon polygon, String label) {
		this.polygon = polygon;
		this.label = label;
	}
	
	public Polygon getPolygon() {
		return polygon;
	}
	public void setPolygon(Polygon polygon) {
		this.polygon = polygon;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
}
