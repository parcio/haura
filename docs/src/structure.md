# Structure

_Haura_ is structured into multiple subprojects for some separation of concerns.
Mainly the can be divided into implementation and bindings.

Implementation:
- [**betree_storage_stack**](./betree.md): The actual implementation and of main interest when
  considering any modifications to the algorithmic makeup of the storage logic
  and interfaces. This crate also contains C bindings.
- [**bectl**](./bectl.md): Allows for a basic acces to the storage stack as an CLI application.

Bindings:
- [**julea-betree**](./julea-betree.md): Bindings exposed to be used by [JULEA](https://github.com/parcio/julea). Specifies a betree backend.
- [**julea-sys**](./julea-sys.md): Generated bindings by bindgen for use in *julea-betree*.
- [**fio-haura**](./fio-haura/mod.md): Engine for fio.
