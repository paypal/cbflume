couchbaseflume
==============

Couchbase Adapter for Flume. 

With this adapter, new/modified documents from Couchbase can be transffered to Hadoop or other systems using Flume.

Configuring flume is trivial.

#1. Edit <flume installation>/conf/configuration.conf. Sample entries

    # Name the components on this agent
      a1.sources = r1
      a1.sinks = k1
      a1.channels = c1

    # Describe/configure the source
      a1.sources.r1.type=com.paypal.utils.cb.flume.CBFlumeSource
      a1.sources.r1.cbserver=http://localhost:8091/pools
      a1.sources.r1.bucket=default
      a1.sources.r1.streamname=cookiestr10
      a1.sources.r1.startdate=-1
      a1.sources.r1.filterpattern=cs_ca,cs_user

      a1.sinks.k1.type=file_roll
      a1.sinks.k1.sink.directory=/tmp/cookie/data
      a1.sinks.k1.sink.rollInterval=120
      a1.sinks.k1.batchSize=5

    # Use a channel which buffers events in memory
      a1.channels.c1.type = memory
      a1.channels.c1.capacity = 1000
      a1.channels.c1.transactionCapacity = 100

    # Bind the source and sink to the channel
      a1.sources.r1.channels = c1
      a1.sinks.k1.channel = c1 

# 2. Copy Flume Adapater cbflume-0.0.1-SNAPSHOT.jar and dependent jar files to <flume>/lib folder. Following are the dependent jar files:

     cbflume-0.0.1-SNAPSHOT.jar
     hadoop-core-1.2.1.jar
     jettison-1.1.jar
     jackson-mapper-asl-1.9.13.jar
     jackson-core-asl-1.9.13.jar
     netty-3.5.12.Final.jar
     spymemcached-2.11.3.jar
     couchbase-client-1.4.2.jar

# 3. Configure Sink to write data to Hadoop or other Sinks. 

# 4. Start flume.
   sample command:
   bin/flume-ng agent --conf conf --conf-file conf/configuration.conf --name a1 -Dflume.root.logger=INFO,console
   

