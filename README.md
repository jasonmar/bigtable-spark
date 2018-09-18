# BigTable Spark Example

## Purpose

This repository provides an example of writing to BigTable from an Apache Spark application.

It uses the BigTable native java library `"com.google.cloud.bigtable" % "bigtable-client-core" % "1.5.0"`


## Instructions

BigTable values are stored as bytes.

You will need a class which can be converted to `Array[Byte]`.

Consider using Protocol Buffers if you are storing a lot of numeric fields and always retrieve all values. Then you can simply use protobuf's marshal to byte array functionality. Reading from BigTable and deserialization will be possible from any language with a gRPC and protobuf library.

See [BigTable#buildMutateRowsRequest](src/main/scala/com/google/cloud/example/BigTable.scala) for an example of builing a native BigTable Protobuf request message.
