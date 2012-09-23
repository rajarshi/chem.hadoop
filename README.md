Sources to perform cheminformatics tasks within the Hadoop framework. It is currently
written using the [JChem](http://www.chemaxon.com/jchem/) library, which you will need to provide. 
The Hadoop code is based on [Apache Hadoop 1.0.3](http://hadoop.apache.org/docs/r1.0.3/)

Compiling
---------

You will need to modify the `build.xml` to point to you version of the JChem jar file. After that
you can generate jar files for specific tasks

* SMARTS matching - `ant smarts-search`
* Bioisostere generation - `ant bioisostere`

Running
--------

Assuming you have your Hadoop cluster and associated HDFS set up, you can run the applications as

	 hadoop jar /path/to/jarfile args

For the bioisostere application you must specify the input file of scaffolds and scaffold members,
the output directory and the path to a license file. The input and license files should have been
copied into the HDFS. The application is then run as

    hadoop jar /path/to/bisos.jar input_file output_dir license.cxl

The format for the scaffold input file is 

    scaff_smi member_smi member_smi ....
