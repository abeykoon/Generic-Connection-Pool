# Generic Connection Pool

This is a generic connection pool implementation where you can use in any Java application to pool
connections or objects. If object creation is costly, definitely pooling will bring better performance for your
application.

## Features

1. BoundedBlockingPool - a fixed size connection pool
2. ExpandingBlockingPool - a connection pool pool with an initial size with ability to
expand and shrink depending on request load
3. Resiliency in creating connections - ability to retry in exponential backoff way when creating connections. This
is useful to handle situations where backend apps can go down and come back. Pool will auto-recover
4. Ability to define how to create connections, close connections and validate connections with clear interfaces
5. Test and borrow support of connections. Applications will never suffer from mal-functioning connections.
6. Ability to extend and write your own connection pool implementation

## How to use

<u>**BoundedBlockingPool**</u>

Create a new BoundedBlockingPool. This is a fixed size thread pool. Connection requests will get blocked until
 a connection is returned (optionally with a timeout).

 * **name** - Name of the pool
 * **size** - Size of the pool
 * **resiliencyParams** - Parameters related to connection retry
 * **validator** - {ConnectionValidator} instance containing logic how to validate connections
 * **connectionFactory** - {ConnectionFactory} instance containing logic how to create connections


<u>**ExpandingBlockingPool**</u>

Create a BoundedBlockingPool. This pool has a initial size and it can create new connections and grow up to a max size depending
on connection requests. However if additional connections are not used they will expire and pool will shrink back
to initial size. Whenever a new connection is requested resiliencyParams are applied and it will repeatedly try to
create connection in exponential back-off manner. Connection requests will get blocked until a connection is
returned (optionally with a timeout).

* **name** - Name of the pool
* **initialSize** - Initial Size of the pool. At the start, pool will create this number of connections.
* **maxSize** - Max size the pool is allowed to grow. This number of connections will get created at max.
* **timeToLive** - TTL of the connections (in seconds). This is used to invalidate additional connections created beyond initialSize
* **resiliencyParams**  - Parameters related to connection retry
* **validator** - {ConnectionValidator} instance containing logic how to validate connections
* **connectionFactory** - {ConnectionFactory} instance containing logic how to create connections






