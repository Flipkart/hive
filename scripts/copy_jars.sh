#!/bin/bash
echo "[INFO] Building"
#cd /Users/jithendhirakumar.r/Flipkart/hive
#MAVEN_OPTS= -XX:+TieredCompilation -XX:TieredStopAtLevel=1
#mvn clean && mvn install -Pdist -Dmaven.javadoc.skip=true -DskipTests=true -T10C
echo "[INFO] Copying to local setup:"
rm -rf /usr/local/myapps/hive-3.1.2.fk/*
cp -r /Users/jithendhirakumar.r/Flipkart/hive/packaging/target/apache-hive-3.1.2.fk.5-bin/apache-hive-3.1.2.fk.5-bin/* /usr/local/myapps/hive-3.1.2.fk/
cp /Users/jithendhirakumar.r/MyPackages/mysql-connector-java-8.0.16.jar /usr/local/myapps/hive-3.1.2.fk/lib/
rm -rf /usr/local/myapps/hive-3.1.2.fk/conf
ln -s /usr/local/myconf/hive-3.1.2.fk /usr/local/myapps/hive-3.1.2.fk/conf
echo "[INFO] Done!"
