# distributed-lock
distributed lock project in java

DLock Properties:
dlock.host (default 127.0.0.1)
dlock.port.min (default 4900)
dlock.port.max (default 5000)
dlock.threadPoolSize (default 2 times number of cpu)


1. Create client instance:

Properties props = new Properties();
props.setProperty("dlock.host", "127.0.0.1");
props.setProperty("dlock.port.min", "4900");
props.setProperty("dlock.port.max", "5000");
props.setProperty("dlock.threadPoolSize", "2");

DLockClient client = new DLockClient(props);


2. Acquire lock:
String lockKey = "key";
long lockTimeout = 1500;
TimeUnit lockTimeoutUnit = TimeUnit.MILLISECONDS;

DLock lock = client.acquireLock(lockKey, lockTimeout, lockTimeoutUnit);
try {
  // do something
} finally {
  lock.release();
}
