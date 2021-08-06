#!/usr/bin/env bash
# encoding: utf-8.0

mvn clean
mvn compile
mvn package

rm ~/Downloads/spark_tag-1.0-SNAPSHOT.jar
mv ./target/spark_tag-1.0-SNAPSHOT.jar ~/Downloads/spark_tag-1.0-SNAPSHOT.jar

sh ~/sh/commit2Git.sh
mvn clean
