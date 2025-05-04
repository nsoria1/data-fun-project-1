package schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType


object Schema {
  val editSchema: StructType = StructType(Seq(
    StructField("name", StringType, nullable = true),
    StructField("identifier", LongType, nullable = true),
    StructField("event", StructType(Seq(
      StructField("type", StringType, nullable = true)
    ))),
    StructField("date_created", StringType, nullable = true))
  )
}
