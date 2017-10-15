# Wholesale Supplier Application

### Introduction

This project is made for module CS4224 Distributed Databases from National University of Singapore.

The aim for this project is to acquire practical experience with using distributed database systems 
for application development.

In this project, an application for a wholesale supplier will be developed using 
[Apache Cassandra](http://cassandra.apache.org/).

Language used: [Java](http://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html)

### Prerequisite

- Download and install [Java 8](http://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html)
- Download and install [Apache Cassandra 3.11.0](http://www.apache.org/dyn/closer.lua/cassandra/3.11.0/apache-cassandra-3.11.0-bin.tar.gz)
- Install [maven](https://stackoverflow.com/questions/7532928/how-do-i-install-maven-with-yum), 
and ensure `mvn --version` command works

### Setup

1. Start cassandra on local machine: `<path to cassandra folder>/cassandra -f`. For nodes, following this guildline https://docs.datastax.com/en/cassandra/2.1/cassandra/initialize/initializeSingleDS.html to setup your clusters. Remeber to update the rpc_address as your IP address instead of 'localhost'.

2. Clone this repo to wherever you wish to run the client (e.g. cs4224e@sunfire.comp.nus.edu.sg). Make a copy of `.env.example` file and rename it to `config.env`.
 
3. For `.env` file, add in values for `CONTACT_POINTS` 
_(if multiple nodes are used, separate them with comma for example: 1.2.3.4,1.2.3.5 )_
and `KEY_SPACE` in `.env` file  _(for example: wholesale_supplier)_,
to accommodate to your machine environment.

4. Create a `data` folder under the root directory if not present, 
put all bootstrap data files in `.csv` format into `data` folder.

5. Create a `xact` folder under the root directory if not present, 
put all pre-defined xact files in `.txt` format into `xact` folder.

6. In project root folder, compile the project via 
`mvn clean dependency:copy-dependencies package`

7. Run `java -Xms2g -Xmx2g -cp target/*:target/dependency/*:. main.java.Setup` once 
to create key space and schemas used in this project. 
Meanwhile, all bootstrap data will be loaded into the local machine.

### Exectution

1. After following above setup steps, execute the main class for the project via 
`java -Xms2g -Xmx2g -cp target/*:target/dependency/*:. main.java.Main`

2. After the program is started, follow instruction and enter number of clients
and read and write consistency level parameters.

3. Program will be executed with log continuously printed in the terminal. 
After all Clients have completed their transactions, `performanceMeasurement.txt` 
will be created under the root folder with performance measurements.
