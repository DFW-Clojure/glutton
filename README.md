# glutton

A Storm project designed to ... well, that part is up to you.

## Usage

Be sure to read up on Storm first:

http://storm-project.net/
https://github.com/nathanmarz/storm/
https://github.com/nathanmarz/storm/wiki

To run on a local cluster:

```bash
lein run -m glutton.topology/run!
# OR
lein run -m glutton.topology/run! debug false workers 10
```

To run on a distributed cluster:

```bash
lein uberjar
# copy jar to nimbus, and then on nimbus:
bin/storm jar path/to/uberjar.jar glutton.TopologySubmitter workers 30 debug false
```

or use `[storm-deploy](https://github.com/nathanmarz/storm-deploy/wiki)`

## License

Copyright © 2013 FIXME

Distributed under the Eclipse Public License, the same as Clojure.