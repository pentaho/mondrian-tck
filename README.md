

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
