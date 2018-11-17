# kcp: copy messages between Kafka topics and files, with optional format conversion

```
$ ./kcp  --help
usage: kcp [-h] [--from {json,avro}] [--to {json,avro}] [--schema SCHEMA]
           [--save-schema SAVE_SCHEMA] [--count COUNT] [--progress]
           sources [sources ...] dest

Consume AVRO or JSON-formatted messages from a source, and produce them to a
destination.

positional arguments:
  sources               Data source.
  dest                  Data destination

optional arguments:
  -h, --help            show this help message and exit
  --from {json,avro}    Input format
  --to {json,avro}      Input format
  --schema SCHEMA       Output schema
  --save-schema SAVE_SCHEMA
                        Save input schema into a file
  --count COUNT         Number of messages to consume
  --progress            Show progress indicator
```

## Source/destination specification

The source and destination can either refer to simple files, or a `kafka://` URL. If given as files, 
the extension will be used to detect the format (`.json` for JSON and `.avro` for AVRO files).

If a `kafka://` URL is given, it must be of the form:
```
kafka://[groupid@]broker[,broker2[,...]]/topicspec[,topicspec[,...]].
```
where:
* `groupid`: is the Kafka consumer group ID
* `broker`: is the DNS name or the IP of the broker (the "bootstrap address"; there can be multiple if this is a broker cluster
* `topicspec`: is the topic specification, a JAVA regular expression (there can be multiple specs, to read 
  more than one matching group of topics)

## File formats

### JSON/JSONL

The inputs can be in `JSON` or `JSONL` format. If it's JSON, the whole file is assumed to contain a single record; multiple 
record inputs must be in JSONL. JSON outputs are always stored as JSONL.

### Avro

Avro files are assumed to follow the usual
[Avro OCF format](https://avro.apache.org/docs/1.8.2/spec.html#Object+Container+Files) specification.

## Prototypical examples

### Get 160 messages from matched topics, write them into an AVRO file
```
$ ./kcp --progress "kafka://epyc.astro.washington.edu/^ztf_20181115_programid.$" mydata.avro --count 160
Generated fake groupid mjuric-NVXU38A7TA1ANRJSNB7N and commit=False
00000000000000000000000000000000000000000000000000000000000000000000000000000000 [      80 @ Nov 16 2018 15:55:06 ]
00000000000000000000000000000000000000000000000000000000000000000000000000000000 [     160 @ Nov 16 2018 15:55:07 ]

Sources read:
  kafka://epyc.astro.washington.edu/ztf_20181115_programid1
$
```

### Get 160 messages from matched topics, write them out as JSON
```
$ ./kcp --progress "kafka://epyc.astro.washington.edu/^ztf_20181115_programid.$" mydata.json --count 160
Generated fake groupid mjuric-I8ET2O7YKDYMA7X1HZZR and commit=False
00000000000000000000000000000000000000000000000000000000000000000000000000000000 [      80 @ Nov 16 2018 15:55:38 ]
00000000000000000000000000000000000000000000000000000000000000000000000000000000 [     160 @ Nov 16 2018 15:55:39 ]

Sources read:
  kafka://epyc.astro.washington.edu/ztf_20181115_programid1
$
```
