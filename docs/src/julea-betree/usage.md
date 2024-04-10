# Usage in JULEA

When the build finishes you'll find a `libobject_betree.so` in the target
directory, either under `Release` or `Debug` depending on your build (`Debug` is
default, so check there when you are unsure).

With the built library, we can switch to JULEA to integrate our results into
their configuration. If you have not setup JULEA have a look at their
[documentation](https://github.com/parcio/julea#quick-start). Continue their
documentation until you configure JULEA with `julea-config`, then this
documentation will use a modified procedure.

## Configure JULEA

For JULEA to use `betree_storage_stack` as its backend (for object storage) we
have to set the appropriate flags for `julea-config`. The example below uses the
user instance of haura as used in [bectl documentation](../bectl/usage.md).

```sh
julea-config --user \
  --object-servers="$(hostname)" --kv-servers="$(hostname)" --db-servers="$(hostname)" \
  --object-backend=betree --object-path="$HOME/.config/haura.json" \
  --kv-backend=lmdb --kv-path="/tmp/julea-$(id -u)/lmdb" \
  --db-backend=sqlite --db-path="/tmp/julea-$(id -u)/sqlite"
```

## Prepare the library

JULEA can't find the library in its current position as it is located in the
build directory of *Haura*. For debugging purposes we can simply copy the
produced `libobject_haura.so` to the JULEA backend directory.

> TODO
> 
> ---
> 
> How JULEA loads backend and where is still work in progress and likely to
> change in the future. This documentation holds for the current progress that
> has been made but only in the `Debug` build of JULEA.

To copy the library to JULEA with the loaded JULEA environment run:

```sh
# The change between underscore and hyphen is not a typo but a mismatch
# between allowed names in cargo and expected names in JULEA.
$ cp target/debug/libobject_betree.so $JULEA_BACKEND_PATH/libobject-betree.so
```


## Start JULEA

To start JULEA, navigate back to it's directory and run

```sh
$ ./scripts/setup.sh start
```

To check if everything worked check your logs best with
```sh
$ journalctl -e GLIB_DOMAIN=JULEA
```

> On success this should look like this (some details will look different on
> your machine):
> ```
> Apr 10 11:46:31 nicomedia julea-server[15742]: Loaded object backend betree.
> Apr 10 11:46:32 nicomedia julea-server[15742]: Initialized object backend betree.
> Apr 10 11:46:32 nicomedia julea-server[15742]: Loaded kv backend lmdb.
> Apr 10 11:46:32 nicomedia julea-server[15742]: Initialized kv backend lmdb.
> Apr 10 11:46:32 nicomedia julea-server[15742]: Loaded db backend sqlite.
> Apr 10 11:46:32 nicomedia julea-server[15742]: Initialized db backend sqlite.
> ```

If everything worked fine so far you can run the JULEA test suite with
```sh
$ ./scripts/test.sh
```

A number of tests are executed then and reports on failures will be generated
for you.
