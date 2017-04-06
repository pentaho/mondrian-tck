

mondrian-tck
===================

mondrian-tck is a set of utilities for testing Mondrian's compatability with various data sources.  Features include:

* Connect to Olap data sources using a JDBC connection string
* Execute arbitrary MDX queries
* Assert the results are correct
* Assert a given SQL query was run during the MDX execution
* Override any Mondrian property for the given test method 

Running the tests
-----------------

To run the Mondrian TCK, you'll first need to checkout the code locally.

```
git clone https://github.com/pentaho/mondrian-tck.git
```

Now edit test.properties at the root of the project. The important properties to configure are those. If a property isn't applicable, do not comment it out; simply leave it blank.

Property | Description | Example
--- | --- | ---
jdbc.drivers | Comma separated list of fully qualified class names of JDBC drivers to initialize in the classloader. | org.apache.hive.jdbc.HiveDriver[,com.example.Driver]
jdbc.url | This is the JDBC URL to use to test SQL and JDBC capabilities. The same URL will also be used to connect to mondrian and perform integration tests. | jdbc:hive2://10.100.1.1:21050/;auth=noSasl
jdbc.user | The username to use when establishing SQL connections to the server. | foodmart
jdbc.password | The password to use when establishing SQL connections to the server. | password
jdbc.extra.parameters | Extra parameters to add to the Mondrian URL when creating connections. Some DBs require special parameters. | PoolNeeded=false


Deploying the test database
---------------------------

This project assumes that the FoodMart dataset has been deployed on the target DB.


Adding drivers to the project
-----------------------------
To run successfully, this TCK requires the drivers of the DB to be present in its classpath. To make the drivers available, add them as a dependency by editing the pom.xml file at the root of this project. It will be downloaded from Maven's central repositories when the tests start.

Alternatively, you can also refer to JAR files on your local system within the pom.xml configuration file.

```
<project>
  ...
  <dependencies>
    <dependency>
      <groupId>mysql</groupId>
      <artifactId>mysql-connector-java</artifactId>
      <version>5.1.31</version>
      <scope>system</scope>
      <systemPath>/usr/lib/jdbc/mysql-bin.jar</systemPath>
    </dependency>
  </dependencies>
  ...
</project>
```

Which version of Mondrian am I testing?
-----------------------------
To choose which version of Mondrian to test, you need to edit the file pom.xml and change the version defined in there.

```
<project>
  ...
  <dependencies>
    <dependency>
      <groupId>pentaho</groupId>
      <artifactId>mondrian</artifactId>
      <version>TRUNK-SNAPSHOT</version> <!-- This is where you pick a version -->
      <scope>compile</scope>
    </dependency>
  </dependencies>
  ...
</project>
```

Testing the Pentaho Big Data Shims
----------------------------------
To use the TCK over the Pentaho Big Data Plugin shims, you will need to verify these properties in test.properties:

```
# These properties are required when testing Pentaho's shims only.
big-data-plugin.folder=pentaho-big-data-plugin
register.big-data-plugin=false
active.hadoop.configuration=cdh50
```

You need only to set 'register.big-data-plugin' to 'true', and the property 'active.hadoop.configuration' must be set to a valid shim identifier.

To test a specific plugin version, you will also need to edit pom.xml:

```
<project>
  ...
  <dependencies>
    <dependency>
      <groupId>pentaho</groupId>
      <artifactId>pentaho-hadoop-hive-jdbc-shim</artifactId>
      <version>5.1.0.0-752</version> <!-- This is where you pick a version -->
    </dependency>
    <dependency>
      <groupId>pentaho-kettle</groupId>
      <artifactId>kettle-core</artifactId>
      <version>5.1.0.0-752</version> <!-- This is where you pick a version -->
    </dependency>
    <dependency>
      <groupId>pentaho-kettle</groupId>
      <artifactId>kettle-engine</artifactId>
      <version>5.1.0.0-752</version> <!-- This is where you pick a version -->
    </dependency>
  </dependencies>

(... and lower down ...)

  <profiles>
    <profile>
      <id>bigdata</id>
      <activation>
        <activeByDefault>true</activeByDefault>
      </activation>
      <dependencies>
        <dependency>
          <groupId>pentaho</groupId>
          <artifactId>pentaho-big-data-plugin</artifactId>
          <type>zip</type>
          <version>5.1.0.0-752</version> <!-- This is where you pick a version -->
          <scope>provided</scope>
        </dependency>
      </dependencies>
```

Extending the test suite
------------------------

There is a default test suite included, but the utilities can be used to create other suite's if needed.

**Example:** 

Create a connection, execute a query, verify the results and verify the SQL
```
final MondrianExpectation expectation =
  MondrianExpectation.newBuilder()
    .query( "Your MDX query here" )
    .result("Expected Results here" )
    .sql( "Expected sql here" )
    .sql( "Another expected sql here")
    .build();
  MondrianContext context = MondrianContext.forConnection(
    "jdbc:mondrian:Catalog=FoodMart.xml;PoolNeeded=false;JDBC=\"jdbc:hive2://host/;auth=noSasl;\"");
  context.verify( expectation );
```

**Example:** 

Create a connection, Override the ResultLimit Property, execute a query.  defaultContext finds its connectString from mondrian.properties
```
new PropertyContext()
  .withProperty( MondrianProperties.instance().ResultLimit, "5" )
  .execute(
    new Runnable() {
      @Override
      public void run() {
        MondrianExpectation expectation = 
          MondrianExpectation.newBuilder().query( "Select from Sales" ).build();
          try {
            MondrianContext.defaultContext().verify( expectation );
          } catch ( Exception e ) {
              //do something
          }
        }
      } );
```

**Example SQL test**

This example demonstrates testing joins between two tables within the WHERE clause. We use the SqlExpectation builder to define the success conditions. Read the Javadoc of SqlExpectation for full details.
```
    final SqlExpectation expct =
      newBuilder()

        // We join two tables in the WHERE clause and select a column in each
        .query( "select warehouse.warehouse_id, warehouse_class.description from warehouse, warehouse_class where warehouse.warehouse_class_id = warehouse_class.warehouse_class_id" )

        // We expect two columns.
        .columns( "warehouse_id", "description" )

        // We want an integer and a string
        .types( Types.INTEGER, Types.VARCHAR )

        // We will validate the first rows content.
        .rows( "1|Small Independent" )

        // We won't validate all rows.
        .partial()
        .build();

    // Validate
    SqlContext.defaultContext().verify( expct );
```
#### Pre-requisites for building the project:
* Maven, version 3+
* Java JDK 1.8
* This [settings.xml](https://github.com/pentaho/maven-parent-poms/blob/master/maven-support-files/settings.xml) in your <user-home>/.m2 directory

#### Building it

__Build for nightly/release__

All required profiles are activated by the presence of a property named "release".

```
$ mvn clean install -Drelease
```

This will build, unit test, and package the whole project (all of the sub-modules). The artifact will be generated in: ```target```

__Build for CI/dev__

The `release` builds will compile the source for production (meaning potential obfuscation and/or uglification). To build without that happening, just eliminate the `release` property.

```
$ mvn clean install
```

#### Running the tests

__Unit tests__

This will run all tests in the project (and sub-modules).
```
$ mvn test
```

If you want to remote debug a single java unit test (default port is 5005):
```
$ cd core
$ mvn test -Dtest=<<YourTest>> -Dmaven.surefire.debug
```

