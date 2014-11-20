# Clickstream & Social Analysis

This repository contains is a couple of examples of using [Apache Spark](http://spark.apache.org/) to
process social media data (JSON) into an abstract 'Interaction' we want to analyse. 

## Acquiring Data

The data used in this example came from streams of Facebook data provided by [Datasift](https://datasift.com/).
While we cannot redistribute the data we demonstrated, you can acquire it yourself
using [Datasift](https://datasift.com/) for around $5 a day.

## Running from Eclipse

If you'd like to import and use this project from Eclipse, make sure you have
SBT 0.13+ installed and run the following:

    sbt eclipse

This will generate the Eclipse project metadata and you can use File -> Import
to load it into your workspace.

## Building a JAR

You can also submit this job to [Apache Spark](http://spark.apache.org/) as a JAR file using `sbt assembly`
to build the project and `spark-submit` to run the Job on your existing cluster.
