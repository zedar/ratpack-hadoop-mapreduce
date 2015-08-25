Ratpack, Hadoop and Mapreduce
-----------------------------

This repository contains the example [Ratpack](http://ratpack.io) application executing Hadoop mapreduce functions.

There are the modules:

* [mapreduce-func](https://github.com/zedar/ratpack-hadoop-mapreduce/tree/master/mapreduce-func) - implementation of
mappers, reducers, combiners. Example algorithms: *Top N the most active users from access logs*.
* [ratpack-app](https://github.com/zedar/ratpack-hadoop-mapreduce/tree/master/ratpack-app) - provides REST API for calling
mapreduce calculation.

You can start the  app with

    ./gradlew run


