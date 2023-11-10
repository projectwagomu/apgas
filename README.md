# Elastic APGAS for Java

This project is an extension of the X10-derived [APGAS for Java](https://github.com/x10-lang/apgas) library capable of dynamically adding and removing places to a running distributed program.

This library enables _malleable_ programs, that is, programs that can modify the number of processes they are currently using following an order coming from the job scheduler. The default implementation relies on sockets to receive _shrink_ and _grow_ orders, but is implemented in a modular whay that will allow easy adaptation to other communication methods between the running AGPAS program and the job scheduler.

Upon receiving a _shrink_ or _grow_ order, the APGAS runtime will transmit this information to the user-implemented `MalleableHandler`. This allows application developers to relocate their necessary data and tasks of their application before and after *Place*s are removed and added to the runtime. A [dummy application](src/main/java/apgas/impl/elastic/DummyApplication.java) along with a [script](bin/testMalleableApplications.sh) demonstrating these mechanisms are provided.

## Requirements

This project requires Java 11 or greater and Maven to compile and run the library.

## Build instructions

This project can be compiled using Maven with the following commands:

```shell
$ mvn verify
```

To compile and add this project to your local Maven repository, use command

```shell
$ mvn install
```

or without running tests

```shell
$ mvn install -DskipTests
```

To generate the Javadoc and other reports about this project, use command

```shell
$ mvn site
```

## Using this project as a dependency

You can this project as a dependency to your own project by including it as a dependency to your project.

In Maven:

```xml
<dependency>
  <groupId>com.github.projectwagomu</groupId>
  <artifactId>apgas</artifactId>
  <version>0.0.2</version>
</dependency>
```

In Gradle:

```
compile 'com.github.projectwagomu:apgas:0.0.2'
```

Note that you will need to *install* this project to your local Maven repository beforehand for the methods described above to work (refer to the build instructions above).

Alternatively, you may use the GitHub repository to download the compiled package automatically.

## License

This software is released under the terms of the [Eclipse Public License v1.0](LICENSE.txt), though it also uses third-party packages with their own licensing terms.

## Publications

- Transparent Resource Elasticity for Task-Based Cluster Environments with Work Stealing [10.1145/3458744.3473361](https://doi.org/10.1145/3458744.3473361)

## Contributors

In alphabetical order:

- Patrick Finnerty
- Takuma Kanzaki
- Jonas Posner
