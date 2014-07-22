mondrian-tck
===================

mondrian-tck is a set of utilities for testing Mondrian's compatability with various data sources.  Features include:

* Connect to Olap data sources using a JDBC connection string
* Execute arbitrary MDX queries
* Assert the results are correct
* Assert a given SQL query was run during the MDX execution
* Override any Mondrian property for the given test method 

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

** Example SQL test **

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
