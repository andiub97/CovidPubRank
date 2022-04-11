# CovidPubRank
<p>
  <img src="https://img.shields.io/badge/Scala-%202.12.15-green" alt="alternatetext">
  <img src="https://img.shields.io/badge/Spark-2.4.5-red" alt="alternatetext">
</p>

Project for the course "Scalable and Cloud Programming" of the University of Bologna, A.Y. 2021/2022.  
The project aims at implementing the grid-based clustering algorithm presented in the reference paper by exploiting the MapReduce paradigm.

# Introduction
The purpose of the project was to implement a page rank application of scientific articles concerning Covid-19.  
Different size of datasets and different page rank algorithms were been used to test the weak scalability and the different performance of the application.\
To test the strong scalability of the algorithms implemented in our project it was been used Google Cloud Platform (GCP) which allowed us to increase the performances by adding or removing resources from the system.
# Steps
The implementation of our application is mainly composed by three main steps:

* Loads a graph's list of edges from a given file path
* Loads node labels from a given file path
* Page rank computation
* Algorithms performance 

## Loads a list of edges from a given file path

A <b>list of edges</b> is define as list of couples (List[(Int, Int)] ) each of which is composed by two integers: the first one identifies the current node's id and the second one the outcome node's id.

## Loads node labels from a given file path

Each node will be contained in a Map structure (Map[Int, String]) in which the key is represented as an integer and identifies the node's id and the value is represented as a string which represents the name of the scientific article.

## Page rank computation

To execute page rank we have used two different classes of algorithms:
* Sequential algorithms, executed on a single node
* Distributed algorithms, executed on differents nodes

### Sequential algorithms

This class of algorithms computes the contribution for each node in a sequential way (one by one) so if the size of the datasets grows up the computation time increases linearly.

###  Distributed algorithms

In this class of algorithms has been used the resilient distributed dataset (RDD), which is a collection of elements partitioned across the nodes of the cluster that can be operated on in parallel.\
The distributed algorithms start with the creation of "outEdges" namely the out edges' data structure and it has type RDD of pairs <b>[(Int, Iterable[Int])]</b>.\
It has been defined a second data structure: <i>pageRank</i>, which is a RDD of pairs <b>[(Int, Float)]</b> that contains the initial contributions for each node (1 / NumOfNodes).\
The RDDs "outEdges" and "pageRank" are partitioned for the parallel computation with an HashPartitioner that takes a single argument: "<i>numPartitions</i>" which defines the number of partitions.\
For each iteration "outEdges" and "pageRank" are joined together in order to get for each source node its destination nodes and rank value. Then is performed a <i>flatMap</i> operation in order to get all destination nodes and assign to them the actual contribution of the source node (rank / num. dest. nodes).\
Once the iteration start finishing the action reduce is performed and the RDD pageRank 's value is updated based on page rank formula.\
The computation of the distributed ranking algorithms is executed through parallelization, spreading the computation of the contributions across the workers.\

## Algorithms performance 

The two classes of algorithms have different time computation performance.




