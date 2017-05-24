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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.application;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.Logger;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CRSAuthorityFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

/**
 * Creates a hexagonal raster
 *
 */
public class HexagonalRasterGenerator {
	private static Logger log = org.apache.logging.log4j.LogManager.getLogger(HexagonalRasterGenerator.class);

	public static void main(String[] args) {
		/**
		 * Read input parameter
		 */
		Options options = new Options();
		options.addOption("h", "help", false, "Display this help page");
		options.addOption(Option.builder("d").longOpt("directory").hasArg().argName("home_directory_file").desc("home directory file").build());
		options.addOption(Option.builder("no").longOpt("north").hasArg().argName("north_bound").desc("north bound").build());
		options.addOption(Option.builder("ea").longOpt("east").hasArg().argName("east_bound").desc("east bound").build());
		options.addOption(Option.builder("so").longOpt("south").hasArg().argName("south_bound").desc("south bound").build());
		options.addOption(Option.builder("we").longOpt("west").hasArg().argName("west_bound").desc("west bound").build());
		options.addOption(Option.builder("c").longOpt("crs").hasArg().argName("crs").desc("crs").build());
		options.addOption(Option.builder("r").longOpt("rastersize").hasArg().argName("raster_size").desc("raster size").build());
		
		launch(options, args);
	}
	
	public static void launch (Options options, String[] args) {
		/**
		 * Initialize input parameters
		 */
		int crs = 32633;
		double west = 64031;
		double east = 679874;
		double south = 5127838;
		double north = 5451301;

		double width = 10000;
		double height = width * Math.sqrt(3) / 2;

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		File homeDirectory = null;
		
		try {
			cmd = parser.parse(options, args);

			if (cmd.hasOption('h')) {
				HelpFormatter helpFormatter = new HelpFormatter();
				helpFormatter.printHelp("java -jar vgi-activity-1.0.jar [OPTION]...", options);
				return;
			}
			if (cmd.hasOption('d')) {
				homeDirectory = new File(cmd.getOptionValue('d'));
				if (!homeDirectory.exists()) {
					homeDirectory = null;
				}
			} else {
				homeDirectory = null;
				log.error("Cannot find home directory");
				System.exit(0);
			}
			if (cmd.hasOption("no")) {
				north = Double.valueOf(cmd.getOptionValue('n'));
			}
			if (cmd.hasOption("ea")) {
				east = Double.valueOf(cmd.getOptionValue('e'));
			}
			if (cmd.hasOption("so")) {
				south = Double.valueOf(cmd.getOptionValue('s'));
			}
			if (cmd.hasOption("we")) {
				west = Double.valueOf(cmd.getOptionValue('w'));
			}
			if (cmd.hasOption('c')) {
				crs = Integer.valueOf(cmd.getOptionValue('c'));
			}
			if (cmd.hasOption('w')) {
				width = Double.valueOf(cmd.getOptionValue('w'));
			}
		} catch (Exception e) {
			HelpFormatter helpFormatter = new HelpFormatter();
			if (e.getMessage() != null) {
				log.error(e.getMessage() + '\n');
			} else {
				log.error("Unknown error", e);
			}
			helpFormatter.printHelp("java -jar vgi-activity-1.0.jar [OPTION]...", options);
		}

		/**
		 * Build feature type
		 */
		SimpleFeatureTypeBuilder featureTypePoint = new SimpleFeatureTypeBuilder();
		featureTypePoint.setName("Hexagon");
		try {
			Hints hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
			CRSAuthorityFactory factory = ReferencingFactoryFinder.getCRSAuthorityFactory("EPSG", hints);
			featureTypePoint.setCRS(factory.createCoordinateReferenceSystem("EPSG:" + crs));
		} catch (NoSuchAuthorityCodeException e1) {
			e1.printStackTrace();
		} catch (FactoryException e1) {
			e1.printStackTrace();
		}
		featureTypePoint.add("geom", Polygon.class);
		featureTypePoint.setDefaultGeometry("geom");
		featureTypePoint.add("id", Long.class);
		SimpleFeatureType featureType = featureTypePoint.buildFeatureType();

		/**
		 * Generate raster
		 */
		DecimalFormat df = new DecimalFormat("000");
		DefaultFeatureCollection raster = new DefaultFeatureCollection("hexagon_raster", featureType);

		double rowOffset = 0.0;
		double columnOffset = 0.0;

		for (int row = 0;; row++) {
			columnOffset = (row % 2 == 0) ? 0 : width / 4 * 3;

			double startY = south + rowOffset;

			double yTop = startY + height;
			double yMiddle = startY + height / 2;
			double yBottom = startY;

			for (int column = 0;; column++) {
				double startX = west + columnOffset + (column * width * 1.5);

				String id = "1" + df.format(row) + df.format(column);

				Coordinate[] coordinatesQuadrant = new Coordinate[7];
				coordinatesQuadrant[0] = new Coordinate(startX, yMiddle);
				coordinatesQuadrant[1] = new Coordinate(startX + width / 4, yBottom);
				coordinatesQuadrant[2] = new Coordinate(startX + width / 4 * 3, yBottom);
				coordinatesQuadrant[3] = new Coordinate(startX + width, yMiddle);
				coordinatesQuadrant[4] = new Coordinate(startX + width / 4 * 3, yTop);
				coordinatesQuadrant[5] = new Coordinate(startX + width / 4, yTop);
				coordinatesQuadrant[6] = coordinatesQuadrant[0];

				/** Assemble feature */
				SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);
				GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
				Polygon hexagon = geometryFactory.createPolygon(geometryFactory.createLinearRing(coordinatesQuadrant), null);
				hexagon.setSRID(crs);
				builder.set("geom", hexagon);
				builder.set("id", id);
				raster.add(builder.buildFeature(id));

				if (startX + width > east)
					break;
			}

			if (startY + height >= north)
				break;

			rowOffset += height / 2;
		}

		final FeatureJSON json = new FeatureJSON(new GeometryJSON(7));
		boolean geometryless = raster.getSchema().getGeometryDescriptor() == null;
		json.setEncodeFeatureCollectionBounds(!geometryless);
		json.setEncodeFeatureCollectionCRS(!geometryless);
		try {
			json.writeFeatureCollection(raster, new FileOutputStream(homeDirectory + "/hexagonal_raster.geojson", false));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		log.info("Raster file written!");
	}
}
