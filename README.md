# XML to JSON-list
Command-line tool for extracting XML payloads into Dataflow-friendly JSON-list files.

## Using the tool
The extractor is published to GCR. Run it with:
```bash
docker run --rm -it \
  us.gcr.io/broad-dsp-gcr-public/monster-xml-to-json-list:<version> --help
```
Version tags match [releases](https://github.com/broadinstitute/monster-xml-to-json-list/releases) in GitHub.

The tool takes the following options:

| Flag | Required? | Description |
| ---- | --------- | ----------- |
| `--input <path>` | yes | Local path to the XML input file |
| `--output <path>` | yes | Local path to the directory where JSON-list files should be written |
| `--gunzip-input` | no | Set if the input XML is gzipped (default is `false`) |
| `--objects-per-part` | no | Max number of JSON objects to write to each JSON-list file in the output |

## Output convention
This tool uses a variation of the [BadgerFish](http://www.sklar.com/badgerfish/) convention to convert
XML to JSON-list. The only difference from the doc linked above is how root tags are handled. Normally
the root tag of an XML document would be converted as a top-level JSON object, and this payload:
```xml
<Root property="foo">
  <Nested property="bar">baz</Nested>
  <Nested property="qux">wibble</Nested>
  <Nested otherThing="123" />
</Root>
```
Would be converted into this payload:
```json
{
  "Root": {
    "@property": "foo",
    "Nested": [
      {
        "@property": "bar",
        "$": "baz"
      },
      {
        "@property": "qux",
        "$": "wibble"
      }
    ],
    "Nested2": {
      "otherThing": "123"
    }
  } 
}
```
This conversion isn't a huge help to Monster's downstream processing (specifically Dataflow),
because the root object makes it difficult for systems to parallelize over the `Nested` elements.

To make downstream processing simpler, we break convention in a few ways:
* Root-level XML attributes are injected into every output record as a nested object, keyed
  by the root-level tag name.
  * Without a root object, each line in the output file(s) can be parsed without extra context.
  * Duplicating the root attributes ensures they can be used for processing any record, when needed.
* Direct children of the root-level XML tag are written into part-files within output directories
  named after their tag values.
  * Dividing the outputs into multiple files helps Dataflow's I/O workers parallelize over the inputs.
  * Grouping by tag allows for specifying input "files" using simple glob-matching.

Finally, the tool writes output JSONs without newlines, so each line in each output file contains an
entire record.

Using this convention, the example above would be written into two output files. The first output
would be written to a `Nested` directory, with contents:
```json
{ "Nested": { "Root": { "@property": "foo" }, "@property": "bar", "$": "baz" } }
{ "Nested": { "Root": { "@property": "foo" }, "@property": "qux", "$": "wibble" } }
```
The second output would be written to a `Nested2` directory, with contents:
```json
{ "Nested2": { "Root": { "@property": "foo" }, "@otherThing": "123" } }
```
