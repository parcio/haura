FIO_RELEASE=3.33
FIO_ARCHIVE=fio-${FIO_RELEASE}.tar.gz
FIO_URL=https://github.com/axboe/fio/archive/refs/tags/${FIO_ARCHIVE}
CC=gcc

all: src/fio-engine-haura.so

src/fio-engine-haura.so: fio-fio-${FIO_RELEASE}
	 ${CC} -Wall -O2 -g -D_GNU_SOURCE -include \
		 fio-fio-${FIO_RELEASE}/config-host.h -shared -rdynamic -fPIC \
         -Wl,--no-as-needed -l betree_storage_stack \
		 -o src/fio-engine-haura.o src/fio-engine-haura.c 

fio-fio-${FIO_RELEASE}:
	wget ${FIO_URL}
	sha256sum -c checksum.txt
	tar xzf ${FIO_ARCHIVE}
	${RM} ${FIO_ARCHIVE}
	cd fio-fio-${FIO_RELEASE} && ./configure


.PHONY: clean fio

fio: fio-fio-${FIO_RELEASE}
	cd fio-fio-${FIO_RELEASE} && make
	cp fio-fio-${FIO_RELEASE}/fio .

clean:
	${RM} -rf fio-fio-${FIO_RELEASE}
