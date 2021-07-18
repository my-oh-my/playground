**Spark-SQL-Executor**

To Anyone, hope you can find this repo somehow helpfull.<br/>

Tried to put some ideas when it comes to writing Spark applications (using Scala) for any reference.<br/>

Basically, Spark (at least, batch) processing is simple in its nature:<br/>
a) we read some data sets (file related/JDBC - any data source that's in line with DataFrameReader) into internal Spark "collections' (RDD/Dataset/DataFrame) we then,<br/>
b) run some transformations against the data and finally,<br/>
c) write our results back to files.<br/>

As stated in a great book, "Spark The Definitive Guide" - "_There is no performance difference between writing SQL queries or writing DataFrame code, they both “compile” to the same underlying plan that we specify in DataFrame code._"<br/>

Try to thing about this statement...<br/>

When it comes to working with data we can write a business logic as SQLs...<br/>
Having this in mind we now only care about this single line;<br/>
```scala
sparkSession.sql("...")
```

...we are no longer responsible for the business-side :)<br/>

<br/><br/>
Take care,<br/>
me
