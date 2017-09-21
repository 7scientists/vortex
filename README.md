# Vortex - A Graph Database Wrapper Library

Vortex is a Python library that provides a uniform interface to graph databases.
It currently supports primarily SQL-based graph representations via
SQLAlchemy, support for other backends like Neo4j, OrientDB and Cassandra is
on the way though.

## Goals

Vortex does not want to replace libraries for specific graph databases, but
instead provide a **minimal** common interface that allows us to work with
graph data from a variety of sources within a single tool. The following
operations are supported:

* Creating vertices and edges in the graph.
* Loading and filtering vertices and edges based on their data.
* Traversing the graph by following edges.

## Current State

Currently, Vortex is in beta but provides good support for SQL-based graphs.

## Compatibility

Vortex is compatible with Python 2.7+ and Python 3.3+.

## License

Vortex is released under a BSD-3 clause license.