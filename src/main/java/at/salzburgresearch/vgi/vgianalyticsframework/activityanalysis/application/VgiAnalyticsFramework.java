package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.application;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.Logger;

public class VgiAnalyticsFramework {
	private static Logger log = org.apache.logging.log4j.LogManager.getLogger(VgiAnalyticsFramework.class);

	public static void main(String[] args) {
		Options options = new Options();
		options.addOption("h", "help", false, "Display this help page");
		options.addOption(Option.builder("importer").longOpt("osm_importer").argName("osm_importer").desc("OSM Importer").build());
		options.addOption(Option.builder("quadtree").longOpt("quadtree_builder").argName("quadtree_builder").desc("Quadtree Builder").build());
		options.addOption(Option.builder("analysis").longOpt("vgi_analysis").argName("vgi_analysis").desc("VGI analysis").build());
		options.addOption(Option.builder("raster").longOpt("hexagonal_raster_generator").argName("hexagonal_raster_generator").desc("Hexagonal raster generator").build());
		/** Settings */
		options.addOption(Option.builder("s").longOpt("settings").hasArg().argName("settings_file").desc("settings file").build());
		options.addOption(Option.builder("p").longOpt("polygons").hasArg().argName("polygon_file").desc("polygon file").build());
		/** OSM History importer */
		options.addOption(Option.builder("o").longOpt("osm").hasArg().argName("osm_history_file").desc("osm history file").build());
		/** Hexagon builder */
		options.addOption(Option.builder("d").longOpt("directory").hasArg().argName("home_directory_file").desc("home directory file").build());
		options.addOption(Option.builder("no").longOpt("north").hasArg().argName("north_bound").desc("north bound").build());
		options.addOption(Option.builder("ea").longOpt("east").hasArg().argName("east_bound").desc("east bound").build());
		options.addOption(Option.builder("so").longOpt("south").hasArg().argName("south_bound").desc("south bound").build());
		options.addOption(Option.builder("we").longOpt("west").hasArg().argName("west_bound").desc("west bound").build());
		options.addOption(Option.builder("c").longOpt("crs").hasArg().argName("crs").desc("crs").build());
		options.addOption(Option.builder("r").longOpt("rastersize").hasArg().argName("raster_size").desc("raster size").build());
		
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            
            if (cmd.hasOption('h')) {
                HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.printHelp("java -jar vgi-analytics-framework-1.0.jar [OPTION]...", options);
                return;
            }
            if (cmd.hasOption("importer")) {
            	OsmHistoryImporter.launch(options, args);
            } else if (cmd.hasOption("quadtree")) {
            	VgiQuadtreeBuilder.launch(options, args);
            } else if (cmd.hasOption("analysis")) {
            	VgiAnalysis.launch(options, args);
            } else if (cmd.hasOption("raster")) {
            	HexagonalRasterGenerator.launch(options, args);
            } else {
                HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.printHelp("java -jar vgi-analytics-framework-1.0.jar [OPTION]...", options);
                return;
            }
            
        } catch (Exception e) {
            HelpFormatter helpFormatter = new HelpFormatter();
            if (e.getMessage() != null) {
                log.error(e.getMessage() + '\n');
            } else {
                log.error("Unknown error",e);
            }
            helpFormatter.printHelp("java -jar vgi-analytics-framework.jar [OPTION]...", options);
        }
	}
}
