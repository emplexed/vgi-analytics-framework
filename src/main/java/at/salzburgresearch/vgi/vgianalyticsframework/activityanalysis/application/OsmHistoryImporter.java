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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.openstreetmap.osmosis.osmbinary.file.BlockInputStream;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.pipeline.IVgiPipelineSettings;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.importer.OsmDataConsumer;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.importer.impl.OsmPbfParser;
import at.salzburgresearch.vgi.vgianalyticsframework.osm.importer.impl.VgiOperationGeneratorDataHandlerImpl;

public class OsmHistoryImporter {
	private static Logger log = Logger.getLogger(OsmHistoryImporter.class);
	
	public static void main(String[] args) {
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:/application-context-osm-op-generator.xml");
		
		Options options = new Options();
		options.addOption("h", "help", false, "Display this help page");
		options.addOption(Option.builder("o").longOpt("osm").hasArg().argName("osm_history_file").desc("osm history file").build());
		options.addOption(Option.builder("s").longOpt("settings").hasArg().argName("settings_file").desc("settings file").build());
		
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            
            File osmHistoryFile = null;
            File settingsFile = null;

            if (cmd.hasOption('h')) {
                HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.printHelp("java -jar vgi-activity-1.0.jar [OPTION]...", options);
                return;
            }
            if (cmd.hasOption('o')) {
            	osmHistoryFile = new File(cmd.getOptionValue('o'));
            	if (!osmHistoryFile.exists()) {
            		log.warn("OSM History file does not exist! (" + osmHistoryFile.getAbsolutePath() + ")");
            		System.exit(0);
            	}
            } else {
                settingsFile = null;
            }
            if (cmd.hasOption('s')) {
                settingsFile = new File(cmd.getOptionValue('s'));
            	if (!settingsFile.exists()) {
            		log.warn("Setting file does not exist! (" + settingsFile.getAbsolutePath() + ")");
            		System.exit(0);
            	}
            } else {
                settingsFile = null;
            }
			
			IVgiPipelineSettings settings = ((IVgiPipelineSettings)ctx.getBean("vgiPipelineSettings"));
			settings.loadSettings(settingsFile);
			
			InputStream input = new FileInputStream(osmHistoryFile);
			OsmPbfParser osmParser = new OsmPbfParser();

			OsmDataConsumer osmDataConsumer = ((VgiOperationGeneratorDataHandlerImpl)ctx.getBean("vgiOperationGeneratorDataHandler"));
			settings.loadSettings(settingsFile);
			osmParser.setSink(osmDataConsumer);
			new BlockInputStream(input, osmParser).process();
			
			log.info("importer done");

		} catch (ParseException e) {
			log.error("Parse Error");
		} catch (IOException e) {
			log.error("File Error");
        } catch (Exception e) {
            HelpFormatter helpFormatter = new HelpFormatter();
            if (e.getMessage() != null) {
                log.error(e.getMessage() + '\n');
            } else {
                log.error("Unknown error",e);
            }
            helpFormatter.printHelp("java -jar vgi-activity-1.0.jar [OPTION]...", options);
        }
	}
}