export JAR='lib/s2013-v1.0.jar'
export MAIN='mapreduce.JobRunner'
export TABLE='WikiShardIndex'
export OUTPUTTABLE='WikiCount'
#export OUTPUTTABLE='WikiCount'
export USER='hadoop'
export PASSWORD='hadoop'
export AUTHS='enwiki'
export INSTANCE='accumulo-1.5.0'
export ZOOKEEPERS='n005:2181,n010:2181,n015:2181'
export LIBJARS='lib/commons-configuration.jar,lib/commons-collections.jar,lib/ext/wikisearch-ingest-1.5.0.jar,lib/ext/protobuf-java-2.3.0.jar'
export MAPCLASS='mapreduce.MapClass'
export REDUCECLASS='mapreduce.ReduceClass'
export COMBINER='true'

# Accumulo --> Plain Text
	
if [ -z $1 ]
then
bin/tool.sh $JAR $MAIN -libjars $LIBJARS -t $TABLE --output $OUTPUTTABLE -u $USER -p $PASSWORD -auths $AUTHS -i $INSTANCE -z $ZOOKEEPERS --mapClass $MAPCLASS --reduceClass $REDUCECLASS 
else
bin/tool.sh $JAR $MAIN -libjars $LIBJARS -t $1 --output $2 -u $USER -p $PASSWORD -auths $AUTHS -i $INSTANCE -z $ZOOKEEPERS
fi

#bin/tool.sh lib/s2013-v1.0.jar mapreduce.WordCount -t s2013v1 --output /s2013 -u hadoop -p hadoop -i Accumulo-1.5.0 -z n005:2180,n010:2180,n015:2181
