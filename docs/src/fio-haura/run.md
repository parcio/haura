# Run fio with Haura

## Important❗ 

To actually run the engine you need to first compile `betree_storage_stack` in
`release mode`, for this execute `cargo build --release` in `betree/`.
Furthermore, your system must be able to find the compiled library. For that,
source the given environment file in `fio-haura/`.

```sh
$ source ./env.sh
```

## Running fio

`fio` can be configured with CLI options and jobfiles, they both have the same
capabilities, therefore for brevity we will use CLI options here.

As an example to perform a simple IOPS test, you can use:
```sh
$ fio --direct=1 --rw=randwrite --bs=4k --ioengine=external:src/fio-engine-haura.o --runtime=10 --numjobs=4 --time_based --group_reporting --name=iops-test-job --eta-newline=1 --size=4G --thread
```

This starts an IO benchmark using `--direct` access in a `--rw=randread` pattern
using a blocksize of `--bs=4k` for each access. Furthermore, haura is specified
as `--ioengine=external:src/fio-engine-haura.o` and runs for `--runtime=10`
seconds with `--numjobs=4`. The total size of IO operations for each thread is
`--size=4GB` which is the upper limit if runtime is not reached.

Most important is the use of `--thread`. Haura ***does not support multiple
processes*** therefore it will error if specifying `numjobs` without `--thread`.

`fio` prints a summary of the results at then end which should look similar to this output:

```
fio --direct=1 --rw=randwrite --bs=4k --ioengine=external:src/fio-engine-haura.o --runtime=10 --numjobs=4 --time_based --group_reporting --name=iops-test-job --eta-newline=1 --size=4G --thread
iops-test-job: (g=0): rw=randwrite, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=haura, iodepth=1
...
fio-3.30
Starting 4 threads
Jobs: 4 (f=4): [w(4)][30.0%][w=12.3MiB/s][w=3147 IOPS][eta 00m:07s]
Jobs: 4 (f=4): [w(4)][54.5%][w=12.7MiB/s][w=3244 IOPS][eta 00m:05s]
Jobs: 4 (f=4): [w(4)][72.7%][w=12.8MiB/s][w=3283 IOPS][eta 00m:03s] 
Jobs: 4 (f=4): [w(4)][90.9%][eta 00m:01s]                          
Jobs: 4 (f=4): [w(4)][100.0%][w=6422KiB/s][w=1605 IOPS][eta 00m:00s]
iops-test-job: (groupid=0, jobs=4): err= 0: pid=46232: Wed Mar  1 13:42:32 2023
  write: IOPS=2182, BW=8729KiB/s (8938kB/s)(85.4MiB/10024msec); 0 zone resets
    clat (nsec): min=12, max=30256, avg=184.83, stdev=460.93
     lat (nsec): min=1122, max=1629.4M, avg=1828274.84, stdev=26162931.82
    clat percentiles (nsec):
     |  1.00th=[   14],  5.00th=[   17], 10.00th=[   57], 20.00th=[   60],
     | 30.00th=[   62], 40.00th=[   64], 50.00th=[   66], 60.00th=[   68],
     | 70.00th=[   99], 80.00th=[  179], 90.00th=[  692], 95.00th=[  812],
     | 99.00th=[ 1272], 99.50th=[ 1672], 99.90th=[ 3088], 99.95th=[ 3568],
     | 99.99th=[23424]
   bw (  KiB/s): min= 1664, max=24480, per=100.00%, avg=10387.06, stdev=1608.07, samples=67
   iops        : min=  416, max= 6120, avg=2596.72, stdev=402.01, samples=67
  lat (nsec)   : 20=5.94%, 50=2.40%, 100=61.71%, 250=14.51%, 500=4.29%
  lat (nsec)   : 750=1.49%, 1000=8.25%
  lat (usec)   : 2=1.07%, 4=0.30%, 10=0.01%, 20=0.01%, 50=0.01%
  cpu          : usr=16.99%, sys=7.73%, ctx=15964, majf=0, minf=383252
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued rwts: total=0,21874,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=1

Run status group 0 (all jobs):
  WRITE: bw=8729KiB/s (8938kB/s), 8729KiB/s-8729KiB/s (8938kB/s-8938kB/s), io=85.4MiB (89.6MB), run=10024-10024msec
```

## More examples

Have a look at the examples directory of `fio` for more usage examples and job
files.

> Disclaimer: Benchmarks performing *only* read queries are incorrect.
> 
> ---
> 
> As haura is implemented as an object/key-value store performing read tests in
> an unstaged job, for example a simple read IOPS test is rather pointless, as
> search queries will simply fail for all keys and only cached nodes are
> traversed copying no data and fetching no data from disk. The implementation
> of objects then does not assume that sparse areas are actually empty but
> rather void of information (all zeroed) resulting in insanely good values.
