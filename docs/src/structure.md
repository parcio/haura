# Structure

_Haura_ is structured into multiple subprojects for some separation of concerns.
Mainly the can be divided into implementation and bindings. The implementation
provides the actual storage stack with betree and bectl. Bindings to be used by
the [JULEA](https://github.com/julea-io/julea) framework are provided with
julea-betree. While the julea-sys bindings are for now only useful for internal
uses and not expected to be used by any other crate.
