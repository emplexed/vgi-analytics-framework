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

package at.salzburgresearch.vgi.vgianalyticsframework.activityanalysis.service.impl;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.apache.log4j.Logger;

public class CSVFileWriter {
	private static Logger log = Logger.getLogger(CSVFileWriter.class);
	
	private Writer fw = null;
	private String filename = "";
	
	public CSVFileWriter(String filename) {
		this.filename = filename;
		do {
			try {
				fw = new FileWriter(filename, false);
			} catch (FileNotFoundException e) {
				log.warn(e);
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			} catch (IOException e) {
				log.error("Exception " + e);
			}
		} while (fw == null);
	}
	
	public CSVFileWriter(String filename, boolean append) {
		this.filename = filename;
		do {
			try {
				fw = new FileWriter(filename, append);
			} catch (FileNotFoundException e) {
				log.warn(e);
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			} catch (IOException e) {
				log.error("Exception " + e);
			}
		} while (fw == null);
	}
	
	public void writeLine(String line) {
		try {
			fw.write(line);
			fw.append(System.getProperty("line.separator"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void closeFile() {
		if (fw != null) {
			try {
				fw.close();
				log.info("File [" +filename + "] written!");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
