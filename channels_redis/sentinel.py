import asyncio
import collections
import itertools
import random
import string
import time
import types

import aioredis

from channels_redis.core import RedisChannelLayer


def _wrap_close(loop, pool):
    """
    Decorate an event loop's close method with our own.
    """
    original_impl = loop.close

    def _wrapper(self, *args, **kwargs):
        # If the event loop was closed, there's nothing we can do anymore.
        if not self.is_closed():
            self.run_until_complete(pool.close_loop(self))
        # Restore the original close() implementation after we're done.
        self.close = original_impl
        return self.close(*args, **kwargs)

    loop.close = types.MethodType(_wrapper, loop)


class SentinelConnectionPool:
    """
    Connection pool manager for the channel layer.

    It manages a set of connections for the given hosts specifications and
    taking into account asyncio event loops.
    """

    def __init__(self, hosts, master):
        self.hosts = hosts
        self.master = master
        self.conn_map = {}
        self.in_use = {}

    def _ensure_loop(self, loop):
        """
        Get connection list for the specified loop.
        """
        if loop is None:
            loop = asyncio.get_event_loop()

        if loop not in self.conn_map:
            # Swap the loop's close method with our own so we get
            # a chance to do some cleanup.
            _wrap_close(loop, self)
            self.conn_map[loop] = []

        return self.conn_map[loop], loop

    async def pop(self, loop=None):
        """
        Get a connection for the given identifier and loop.
        """
        conns, loop = self._ensure_loop(loop)
        if not conns:
            sentinel = await aioredis.create_sentinel(self.hosts, loop=loop)
            conns.append(sentinel.master_for(self.master))
        conn = conns.pop()
        self.in_use[conn] = loop
        return conn

    def push(self, conn):
        """
        Return a connection to the pool.
        """
        loop = self.in_use[conn]
        del self.in_use[conn]
        if loop is not None:
            conns, _ = self._ensure_loop(loop)
            conns.append(conn)

    def conn_error(self, conn):
        """
        Handle a connection that produced an error.
        """
        conn.close()
        del self.in_use[conn]

    def reset(self):
        """
        Clear all connections from the pool.
        """
        self.conn_map = {}
        self.in_use = {}

    async def close_loop(self, loop):
        """
        Close all connections owned by the pool on the given loop.
        """
        if loop in self.conn_map:
            for conn in self.conn_map[loop]:
                conn.close()
                await conn.wait_closed()
            del self.conn_map[loop]

        for k, v in self.in_use.items():
            if v is loop:
                self.in_use[k] = None

    async def close(self):
        """
        Close all connections owned by the pool.
        """
        conn_map = self.conn_map
        in_use = self.in_use
        self.reset()
        for conns in conn_map.values():
            for conn in conns:
                conn.close()
                await conn.wait_closed()
        for conn in in_use:
            conn.close()
            await conn.wait_closed()


class ChannelLock:
    """
    Helper class for per-channel locking.

    Once a lock is released and has no waiters, it will also be deleted,
    to mitigate multi-event loop problems.
    """

    def __init__(self):
        self.locks = collections.defaultdict(asyncio.Lock)
        self.wait_counts = collections.defaultdict(int)

    async def acquire(self, channel):
        """
        Acquire the lock for the given channel.
        """
        self.wait_counts[channel] += 1
        return await self.locks[channel].acquire()

    def locked(self, channel):
        """
        Return ``True`` if the lock for the given channel is acquired.
        """
        return self.locks[channel].locked()

    def release(self, channel):
        """
        Release the lock for the given channel.
        """
        self.locks[channel].release()
        self.wait_counts[channel] -= 1
        if self.wait_counts[channel] < 1:
            del self.locks[channel]
            del self.wait_counts[channel]


class UnsupportedRedis(Exception):
    pass


class RedisSentinelChannelLayer(RedisChannelLayer):
    """
    Redis Sentinel channel layer.

    It routes all messages into remote Redis server. Support for
    sharding among different Redis installations and message
    encryption are provided.
    """

    brpop_timeout = 5

    def __init__(
        self,
        hosts=None,
        masters=None,
        prefix="asgi:",
        expiry=60,
        group_expiry=86400,
        capacity=100,
        channel_capacity=None,
        symmetric_encryption_keys=None,
    ):
        # Store basic information
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.capacity = capacity
        self.channel_capacity = self.compile_capacities(channel_capacity or {})
        self.prefix = prefix
        assert isinstance(self.prefix, str), "Prefix must be unicode"
        # Configure the host objects
        self.hosts, self.masters = self.decode_shards(hosts, masters)
        self.ring_size = len(self.hosts)
        # Cached redis connection pools and the event loop they are from
        self.pools = [SentinelConnectionPool(sentinel_hosts, master_name) for sentinel_hosts, master_name in zip(self.hosts, self.masters)]
        # Normal channels choose a host index by cycling through the available hosts
        self._receive_index_generator = itertools.cycle(range(len(self.hosts)))
        self._send_index_generator = itertools.cycle(range(len(self.hosts)))
        # Decide on a unique client prefix to use in ! sections
        # TODO: ensure uniqueness better, e.g. Redis keys with SETNX
        self.client_prefix = "".join(
            random.choice(string.ascii_letters) for i in range(8)
        )
        # Set up any encryption objects
        self._setup_encryption(symmetric_encryption_keys)
        # Number of coroutines trying to receive right now
        self.receive_count = 0
        # The receive lock
        self.receive_lock = None
        # Event loop they are trying to receive on
        self.receive_event_loop = None
        # Buffered messages by process-local channel name
        self.receive_buffer = collections.defaultdict(asyncio.Queue)
        # Detached channel cleanup tasks
        self.receive_cleaners = []
        # Per-channel cleanup locks to prevent a receive starting and moving
        # a message back into the main queue before its cleanup has completed
        self.receive_clean_locks = ChannelLock()

    def decode_shards(self, shards, masters):
        """
        Takes the value of the "shards" argument passed to the class and returns
        a list of kwargs to use for the Redis connection constructor.
        """
        # If no master names were provided, return error
        if not masters:
            raise ValueError(
                "You must pass a list of Redis Sentinel Monitored master names for each shard(group of sentinel hosts) provided in hosts"
            )
        if not len(masters) == len(shards):
            raise ValueError(
                "You must pass a list of Redis Sentinel Monitored master names for each shard(group of sentinel hosts) provided in hosts"
            )
        # If no hosts were provided, return a default value
        if not shards:
            return [[("localhost", 6379)]]
        # If they provided just a string, scold them.
        if isinstance(shards, (str, bytes)):
            raise ValueError(
                "You must pass a list of Redis shards containing a list of Redis sentinel Nodes, even if there is only one."
            )
        # Decode each hosts entry into a kwargs dict
        hosts = []
        for entry in shards:
            if isinstance(entry, list):
                hosts.append(entry)
            else:
                raise ValueError(
                    "You must pass a list of Redis shards containing a list of Redis sentinel Nodes, even if there is only one."
                )
        return hosts, masters

    def _setup_encryption(self, symmetric_encryption_keys):
        # See if we can do encryption if they asked
        if symmetric_encryption_keys:
            if isinstance(symmetric_encryption_keys, (str, bytes)):
                raise ValueError(
                    "symmetric_encryption_keys must be a list of possible keys"
                )
            try:
                from cryptography.fernet import MultiFernet
            except ImportError:
                raise ValueError(
                    "Cannot run with encryption without 'cryptography' installed."
                )
            sub_fernets = [self.make_fernet(key) for key in symmetric_encryption_keys]
            self.crypter = MultiFernet(sub_fernets)
        else:
            self.crypter = None

    ### Connection handling ###

    def connection(self, index):
        """
        Returns the correct connection for the index given.
        Lazily instantiates pools.
        """
        # Catch bad indexes
        if not 0 <= index < self.ring_size:
            raise ValueError(
                "There are only %s hosts - you asked for %s!" % (self.ring_size, index)
            )
        # Make a context manager
        return self.ConnectionContextManager(self.pools[index])

    class ConnectionContextManager:
        """
        Async context manager for connections
        """

        def __init__(self, pool):
            self.pool = pool

        async def __aenter__(self):
            self.conn = await self.pool.pop()
            return self.conn

        async def __aexit__(self, exc_type, exc, tb):
            if exc:
                self.pool.conn_error(self.conn)
            else:
                self.pool.push(self.conn)
            self.conn = None
