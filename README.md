# VGI Analyzer

## Overview
The VGI analyzer is an analytics framework for Volunteered Geographic Information (VGI) datasets written in Java. Currently, the tools can handle historic OpenStreetMap datasets.

## Getting Started
1. You can build `vgi-analytics-framework.jar` with maven or directly download it from the `build`-directory.
2. Download the current [OSM Full History File](http://planet.openstreetmap.org/pbf/full-history/) in PBF format!
3. Generate VGI operations! Run `OsmHistoryImporter`. Use option -o to specify the path to the OSM PBF file and the option -s to refer to the settings file which contains the path to the output directory. The PBF files containing VGI operations will be created in this output directory. The settings file can also specify a spatial filter (setting-key `filterPolygon`).
4. Create the spatial index structure (quadtree)! Run `VgiQuadtreeBuilder`. Use option -s to refer to the settings file which contains the path to the VGI data directory. The file-based quadtree will be built inside this directory. Make sure that enough disk space is available (approx. 300 GB). The settings file can also specify a spatial filter (setting-key `filterPolygon`).
5. Run VGI analysis! Run `VgiAnalysis`. Use option -s to refer to the settings file and -p to refer to an optional polygon raster file. The polygon raster file specifies a set of polygons along with a label and can be used to run an analysis for multiple polygons with the same settings (batch processing). The polygon file contains one entry per line. The first value is the polygon geometry (WKT format) and the second value represents the label. Both are separated by a semicolon.

## Analysis Methods
* VgiAnalysisBatchGeneral
* VgiAnalysisBatchContributor
* VgiAnalysisActionPerType
* VgiAnalysisActionPerFeatureType
* VgiAnalysisOperationPerType
* VgiAnalysisUserPerActions: Parameter: `mergeActionTypes`: '`true`' if overall number of actions should be counted; '`false`' if number of action per action type should be counted
* VgiAnalysisUserPerOperations: Parameter: `mergeOperationTypes`: '`true`' if overall number of actions should be counted; '`false`' if number of action per action type should be counted
* VgiAnalysisUserPerTags
* VgiAnalysisChangeDetection
* VgiAnalysisTags: Parameters: `tagKey`: '' if tag keys should be analyzed; '`tag key`' (e.g. building) if tag values should be analyzed 
* VgiAnalysisHourOfDay
* VgiAnalysisFeatureS
* VgiAnalysisActionDetails: Parameter: `includeOperationDetails`: '`true`' if also operation details should be written; '`false`' if only action details should be written

## Notes
* Example setting files are available in the directory `src/main/resources/settings`
* The setting files can be validated using the file `validator.xsd`
* Use the spatial filter to avoid processing the whole planet file which takes much time and requires advanced hardware resources. 
* Assign more memory (`-Xmx8g`) and use the spatial filter (`filterPolygon`) to restrict the amount of data.

## Further Reading
Rehrl, K., Gröchenig, S., 2016, *A framework for data-centric analysis or mapping activity in the context of volunteered geographic information*, ISPRS International Journal of Geo-Information (Open Access)

Rehrl, K., Brunauer, R., Gröchenig, S., 2015, *Towards a Qualitative Assessment of Changes in Geographic Vector Datasets*. In: AGILE 2015 (pp. 181-197). Springer International Publishing

Gröchenig S., Brunauer R., Rehrl K., 2014, *Estimating Completeness of VGI Datasets by Analyzing Community Activity over Time Periods*, In: Connecting a Digital Europe Through Location and Place, Lecture Notes in Geoinformation and Cartography 2014, pp 3-18

Rehrl, K., Gröchenig, S., Hochmair, H. Leitinger, S., Steinmann, R. and Wagner, A., 2012, *A conceptual model for analyzing contribution patterns in the context of VGI*, In: LBS 2012 – 9th Symposium on Location Based Services. Berlin: Springer.