# Basic Repo for working with Spark + Scala

The purpose of this repo is to gain insights about different types of data.

There are a number of Parquet files containing 2016 crime data from four United States cities in resources folder::

Los Angeles
Philadelphia
Dallas
The data is cleaned up a little, but has not been normalized. Each city reports crime data slightly differently,
so examine the data for each city to determine how to query it properly.

Your job is to use some of this data to gain insights about certain kinds of crimes.

## Pre-requisites
Please make sure you have the following installed
* Java 8
* Scala 2.11
* Sbt 1.1.x
* Apache Spark 2.4 with ability to run spark-submit locally

## Setup for local building and testing
* Clone this repo
* Build: sbt package
* Test: sbt test

## Goal
* Fix the test.

