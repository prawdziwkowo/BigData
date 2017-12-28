#!/usr/bin/env bash
rm -rf gen-javabean src/java/manning/schema
thrift -r --gen java:beans,hashcode,nocamel src/schema.thrift
mv gen-javabean src/main/java/manning/schema
rm -rf gen-javabean