# Haura

This book is dedicated as a Getting Started or Developer Guide to the *Haura*
research storage stack. We give you here a quick overview of how *Haura* is used
and its basic structure. If you are intereted in any of the specific parts and
functions of the project navigate in the "Structure" menu on the left to your
desired section.

Haura works different from file system such as ext4 or btrfs which would run in
the kernel. Haura only runs in the userspace and is not designed to be
permanently active nor provide usual interfaces like POSIX. Rather you use a
**key-value** and **object** interface common to databases and mass storage
applications.

Below we lay out two scenarios which show how Haura is commonly used in
conjunction with any arbitrary client code.

## Scenario 1 - Using Haura directly

```dot process
digraph {
    bgcolor="transparent"
    rankdir="LR"
    cluster="True"
    style="dashed"
    
    subgraph Own {
        label="Client Process"
        "Haura" [shape="cylinder"]
        "System Resources" [shape="box"]

        "Haura" -> "System Resources"
        "Client 1" -> "Haura"
    }
}
```

The simplest way to use Haura, as you will also see in the examples, is the
direct initialization of Haura with your own configuration. This will let Haura
run in your own process context meaning that with the termination of your
program Haura will be stopped as well. 

## Scenario 2 - Using a Wrapper

```dot process
digraph {
    bgcolor="transparent"
    rankdir="LR"
    cluster="True"
    style="dashed"
    
    
    subgraph Foreign{
        label="JULEA Process"
        node[shape="cylinder"]

        "JULEA"
        "Haura"
        "System Resources" [shape="box"]

        "JULEA" -> "Haura"
        
        "Haura" -> "System Resources"
    }
    
    subgraph Process1 {
        label="Client Process"
        "Client 1" -> "JULEA"
    }
    subgraph Process2 {
        label="Client Process"
        "Client 2" -> "JULEA"
    }
    subgraph Process3 {
        label="Client Process"
        "Client 3" -> "JULEA"
    }
}
```

Using a wrapper around the functionality like JULEA will allow for more flexible
usage of the storage stack as the JULEA process will own and run the Haura
instance, meaning that the storage stack will surpass the lifetime of an
application process. Additionally, this allows for more than one client to be
connected to an active Haura instance.
