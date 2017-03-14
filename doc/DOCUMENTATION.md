# Documentation

These commands were run on Ubuntu 16.04.2 LTS.

## 1. Install the prerequisites

```
$ sudo apt-get update
$ sudo apt-get install default-jre
$ sudo apt-get install default-jdk
$ sudo apt-get install git
```

## 2. Getting Started

### Clone the GitHub repository

```
$ git clone https://github.com/SGroe/vgi-analytics-framework
$ cd vgi-analytics-framework
```

### Create required directories

```
$ mkdir pbf
$ mkdir results
$ mkdir settings
```

### Download OSM History File

For the purposes of this documentation, we will use an OSM history file for Andorra from Geofabrik.

```
$ wget -P pbf http://download.geofabrik.de/europe/andorra.osh.pbf
```
### Copy desired settings XML file

For the purposes of this documentation, we will use the ```settings_batch.xml``` file.

```
$ cp src/src/main/resources/settings/settings_batch.xml settings
$ cp src/src/main/resources/settings/validator.xsd settings
```
### Edit the settings XML file

```
$ sudo nano settings/settings_batch.xml
```

Edit ```pbfDataFolder```, ```readQuadtree``` and ```resultFolder``` to your settings.

```xml
<general
		settingName="Batch Default"
		pbfDataFolder=" < PATH TO PBF FOLDER > "
		readQuadtree="true OR false"
		resultFolder=" < PATH TO RESULTS FOLDER > "
		filterTimestamp="2020-01-01T00:00:00Z" />
```

**Example:**

```xml
<general
		settingName="Batch Default"
		pbfDataFolder="/home/user/vgi-analytics-framework/pbf"
		readQuadtree="false"
		resultFolder="/home/user/vgi-analytics-framework/results"
		filterTimestamp="2020-01-01T00:00:00Z" />
```
## 3. Running the framework

The ```-s``` option specifies the XML file located in ```/home/user/vgi-analytics-framework/settings```. The ```-o``` option specifies the OSM history file location in ```/home/user/vgi-analytics-framework/pbf```.

### Importer

```
$ cd build
$ java -jar vgi-analytics-framework-0.1.jar -importer -s ../settings/settings_batch.xml -o ../pbf/andorra.osh.pbf
```

### Quadtree
```
$ java -jar vgi-analytics-framework-0.1.jar -quadtree -s ../settings/settings_batch.xml
```

### Analysis
```
$ java -jar vgi-analytics-framework-0.1.jar -analysis -s ../settings/settings_batch.xml 
```

The output CSV files will be located in the ```/home/user/vgi-analytics-framework/results``` folder.
