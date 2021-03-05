package com.trendcore.learning.apache.spark.dataframe;

public class PartitionByVsBucketBy {

    public static void main(String[] args) {
        /*
            peopleDF
              .write
              .partitionBy("favorite_color")    - Will split data using this column
              .bucketBy(42, "name")             - will split data on name column with 42
              .saveAsTable("people_partitioned_bucketed")



              Partitioning

                Based on values of columns of a table, Partition divides large amount of data into multiple slices.
                What that means is we are able to differentiate a large amount of data on the basis of our need,
                for example if we have the data for all the employees working in a particular company ( with huge number of employees)
                but we need to survey only the employees which belong to a particular category,
                in the absence of partitioning our process would be to scan through all the entries and find those out,
                but if we partition our table on the basis of category then it becomes very simple to survey the lot.

                Bucketing

                Bucketing basically puts data into more manageable or equal parts.
                When we go for partitioning, we might end up with multiple small partitions based on column values.
                But when we go for bucketing, we restrict number of buckets to store the data ( which is defined earlier).

                Difference and Conclusion

                When we are dealing with some field in our data which has high cardinality
                ( number of possible values the field can have) it should be taken care that partitioning is not used.
                If we partition a field with large amount of values, we might end up with too many directories in our file system.
                What bucketing does differently to partitioning is we have a fixed number of files,
                since you do specify the number of buckets, then hive will take the field,
                calculate a hash, which is then assigned to that bucket.
                We can partition on multiple fields ( category, country of employee etc),
                while you can bucket on only one field.

                So, bucketing is useful for the situation in which the field has high cardinality
                and data is evenly spread among all buckets ( approximately).
                Partitioning works best when the cardinality of the partitioning
                field is not too high and it can quickly be queued after.
         */
    }

}
