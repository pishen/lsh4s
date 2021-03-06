# lsh4s
[![Build Status](https://travis-ci.org/pishen/lsh4s.svg?branch=master)](https://travis-ci.org/pishen/lsh4s)

## Usage
Add the dependency
```
libraryDependencies += "net.pishen" %% "lsh4s" % "0.6.0"
```
Add the resolver
```
resolvers += Resolver.bintrayRepo("pishen", "maven")
```
Hash the vectors (the whole hashing process will run in memory, you may need to enlarge your JVM's heap size.)
```
import lsh4s._

val lsh = LSH.hash("./input_vectors", numOfHashGroups = 10, bucketSize = 10000, outputPath = "mem")

val neighbors: Seq[Long] = lsh.query(itemId, maxReturnSize = 30).get.map(_.id)
```
* The format of `./input_vectors` is `<item id> <vector>` for each line, here is an example:
```
3 0.2 -1.5 0.3
5 0.4 0.01 -0.5
0 1.1 0.9 -0.1
2 1.2 0.8 0.2
```
* All the hash groups will be combined in the end to find the neighbors, larger `numOfHashGroups` will produce a more accurate model, but takes more memory when hashing.
* Larger `bucketSize` will produce a more accurate model as well, but takes more time when finding neighbors.
* `outputPath = "mem"` is for memory mode, otherwise it will be the output file for LSH model (we recommend pointing this file to an empty directory, since we will create and delete several intermediate files around it.)

lsh4s uses slf4j, remember to add your own backend to see the log. For example, to print the log on screen, add
```
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.14"
```
