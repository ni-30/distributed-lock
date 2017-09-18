 # distributed-lock  #
distributed lock project in java

## DLock Properties: ##
    dlock.host (default 127.0.0.1)
    dlock.port.min (default 4900)
    dlock.port.max (default 5000)
    dlock.threadPoolSize (default 2 times number of cpu)


## Create service instance: ##

    Properties props = new Properties();
    props.setProperty("dlock.host", "127.0.0.1");
    props.setProperty("dlock.port.min", "4900");
    props.setProperty("dlock.port.max", "5000");
    props.setProperty("dlock.threadPoolSize", "2");

    DLockService service = new DLockService(props);
    service.start();


## Acquire lock: ##
    String lockKey = "dummy";
    long waitTime = 1500;
    long leaseTime = 3000;
    TimeUnit timeUnit = TimeUnit.MILLISECONDS;

    DLock lock = service.getLock(lockKey);
    try {
      lock.tryLock(waitTime, leaseTime, timeUnit);
      // do something
    } finally {
      lock.unlock();
    }
