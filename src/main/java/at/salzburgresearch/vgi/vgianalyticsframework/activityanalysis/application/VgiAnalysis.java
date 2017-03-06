/** Copyright 2017, Simon Gröchenig, Salzburg Research Forschungsgesellschaft m.b.H.

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.model.vgi.impl.VgiPolygon;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipeline;
import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;

/**
 * Main class to launch VGI analysis<br>
 * <br>
 * -s path to settings XML file<br>
 * -p path to polygon CSV file
 *
 */
public class VgiAnalysis {
	private static Logger log = Logger.getLogger(VgiAnalysis.class);

	public static void main(String[] args) {
		/**
		 * Read input parameter
		 */
		Options options = new Options();
		options.addOption("h", "help", false, "Display this help page");
		/** Settings */
		options.addOption(Option.builder("s").longOpt("settings").hasArg().argName("settings_file").desc("settings file").build());
		options.addOption(Option.builder("p").longOpt("polygons").hasArg().argName("polygon_file").desc("polygon file").build());
		
		launch(options, args);
	}

	public static void launch (Options options, String[] args) {
		
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try (ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(
				"classpath:/application-context-vgi-pipeline.xml")) {
			cmd = parser.parse(options, args);

			File settingsFile = null;
			File polygonFile = null;
			boolean batchProcessing = false;

			if (cmd.hasOption('h')) {
				HelpFormatter helpFormatter = new HelpFormatter();
				helpFormatter.printHelp("java -jar vgi-activity-1.0.jar [OPTION]...", options);
				return;
			}
			if (cmd.hasOption('s')) {
				settingsFile = new File(cmd.getOptionValue('s'));
				if (!settingsFile.exists()) {
					log.warn("Setting file does not exist! (" + settingsFile.getAbsolutePath() + ")");
					settingsFile = null;
				}
			} else {
				settingsFile = null;
				log.warn("No setting file specified! Use option -s to specify a settings XML file");
				System.exit(0);
			}
			if (cmd.hasOption('p')) {
				polygonFile = new File(cmd.getOptionValue('p'));
				if (polygonFile.exists()) {
					batchProcessing = true;
				} else {
					log.warn("Polygon file does not exist!");
				}
			} else {
				polygonFile = null;
			}

			WKTReader wktReader = new WKTReader();

			IVgiPipeline pipeline = ((IVgiPipeline) ctx.getBean("vgiPipeline"));
			
			IVgiPipelineSettings settings = ((IVgiPipelineSettings) ctx.getBean("vgiPipelineSettings"));
			if (!settings.loadSettings(settingsFile)) {
				System.exit(1);
			}

			if (batchProcessing) {
            	if (settings.getFilterPolygonList() == null) {
            		settings.setFilterPolygonList(new ArrayList<VgiPolygon>());
            	}
            	
				try {
					BufferedReader fileReader = new BufferedReader(new FileReader(polygonFile));

					String line = "";

					while ((line = fileReader.readLine()) != null) {

						String[] split = line.split(";");
						try {
							settings.getFilterPolygonList().add(new VgiPolygon((Polygon) wktReader.read(split[0]), split[1]));

						} catch (ParseException e) {
							log.warn("Cannot parse geometry!! (" + split[0] + ")");
							continue;
						}
					}
					fileReader.close();

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			if (settings.getFilterPolygonList() == null) {
				pipeline.start();
				
			} else {
				log.info(settings.getFilterPolygonList().size() + " filter polygons found!");
				for (VgiPolygon polygon : settings.getFilterPolygonList()) {
					settings.setCurrentPolygon(polygon);
					log.info("Start analysis... " + settings.getCurrentPolygon().getLabel());
					pipeline.start();
				}
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
	}
}
