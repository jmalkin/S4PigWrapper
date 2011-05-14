Pig Wrapper for S4 PEs
======================

Introduction
------------

S4PigWrapper wraps your S4 processing elements (PEs) so that they can
run offline on a Hadoop cluster.  Specifically, this wrapper allows
PEs to be treated as user-defined functions (UDFs) in Pig scripts, Pig
Latin works as a glue language, allowing one to specify how messages
flow between different PEs.

For more information on S4, see [s4.io](http://s4.io).  For more
information on Pig, see [pig.apache.org](http://pig.aparche.org) and
for Hadoop, see [hadoop.apache.org](http://hadoop.apache.org)

See [S4PigExample](github.com/jmalkin/S4PigExample) for an example of
how this wrapper can be used.

Requirements
------------

* Linux
* Java 1.6
* Maven
* s4-core-0.3-SNAPSHOT
* Pig 0.8 or higher
* Hadoop 0.20 or higher

Build Instructions
------------------

1. Either download the S4 tarball from the S4 website, or download the
source code from Github and follow the build instructions.
      - If you download the pre-made tarball, add the s4-core jar to
        your local Maven repository.
      - If you build your own copy, build and install the files to
        your local repository with ./gradlew install

2. Build S4PigWrapper using Maven

      mvn package assembly:single

3. Locate all needed libraries for grid use
      - target/S4PigWrapper-0.1.dir/lib contains all the jars individually
      - target/S4PigWarpper-0.1.tar.gz is a tarball containing all the jars
