# Basic Usage

To use the storage stack via `bectl` you will first need to define a valid
configuration. This can be done either by modifying a small example like shown
below or by creating a valid configuraiton with the `betree_storage_stack` crate
and calling `write_config_json` on the created `Database` object.

##### Example Configuration
```json
{
  "storage": {
    "tiers": [
      {
        "top_level_vdevs": [
          {
            "path": "/tmp/disk_a",
            "direct": true
          }
        ],
        "preferred_access_type": "Unknown"
      }
    ],
    "queue_depth_factor": 20,
    "thread_pool_size": null,
    "thread_pool_pinned": false
  },
  "alloc_strategy": [
    [
      0
    ],
    [
      1
    ],
    [
      2
    ],
    [
      3
    ]
  ],
  "default_storage_class": 0,
  "compression": "None",
  "cache_size": 4294967296,
  "access_mode": "OpenOrCreate",
  "sync_interval_ms": null,
  "migration_policy": null,
  "metrics": null
}
```

Store this configuration in a convenient place for example, if defined, under
`$XDG_CONFIG_HOME/haura.json` or alternatively `$HOME/.config/haura.json`. To
avoid specifying this path on each access, store the location of your
configuration in your environment as `$BETREE_CONFIG`. For example as so:

```sh
# excerpt from .your_favorite_shellenv

export BETREE_CONFIG=$HOME/.config/haura.json
```

> EDITOR NOTE: We may want to specify this as a `yaml` or `toml` in the future to ease the
> configuration and actually be able to write this by hand besides the zfs-like
> configuration string.

## Usage

Once you have configured everything you are ready to use `bectl`. We show you
here the very basics on how to start using the CLI, you may find further in the
CLI itself from various help pages.

### Initialization

Before using you'll need to initialize your storage.

```sh
$ bectl db init
```

### Write Data

To write some value *baz* in the dataset *foo* in key *bar*.

```sh
bectl kv foo put bar baz
```

### Read Data

To read some data from dataset *foo* in key *bar*.

```sh
bectl kv foo get bar
```

