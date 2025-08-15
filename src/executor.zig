const std = @import("std");
const Instant = std.time.Instant;
const Allocator = std.mem.Allocator;
const linux = std.os.linux;

const Queue = @import("./queue.zig").Queue;
const SliceMap = @import("./slice_map.zig").SliceMap;
const Slab = @import("./slab.zig").Slab;

pub const Executor = struct {};

const IoUring = struct {
    const Self = @This();

    ring: linux.IoUring,

    // Queued to be pushed to the sq
    io_queue: Queue(linux.io_uring_sqe),

    // Finished io results kept as user_data -> return_value mapping
    io_results: SliceMap(u64, i32),

    // Keep submission time of IO so we can check if any IO is taking too much time.
    io_submit_time: SliceMap(u64, Instant),

    pending_io: u16,

    pub fn init(params: struct {
        /// Limit for number of io running at any given time. Must be greater than zero.
        depth: u16,
        /// Can pass a reference to another engine here to make engines share the underlying kernel worker pool.
        ///
        /// There can be CPU usage problems if each engine uses it's own without sharing and there are many engines because we use SQPOLL with uring which means
        /// the kernel will have a busy polling thread for each engine.
        buddy_engine: ?*const Self,
        /// Use polled io. if this argument is true then the engine can only be used for writing/reading from sockets(napi) and files(direct_io)
        polled_io: bool,
        /// Allocator to be used for allocating internal data structures, same allocator should be passed to the deinit method
        alloc: Allocator,
    }) error{OutOfMemory}!Self {
        std.debug.assert(params.depth > 0);

        var flags: u32 = linux.IORING_SETUP_SQPOLL | linux.IORING_SETUP_COOP_TASKRUN | linux.IORING_SETUP_SINGLE_ISSUER;

        if (params.polled_io) {
            flags |= linux.IORING_SETUP_IOPOLL;
        }

        if (params.buddy_engine != null) {
            flags |= linux.IORING_SETUP_ATTACH_WQ;
        }

        var ring_params = std.mem.zeroInit(linux.io_uring_params, .{
            .wq_fd = if (params.buddy_engine) |buddy| @intCast(buddy.ring.fd) else 0,
            .sq_thread_idle = 1000,
            .flags = flags,
        });
        const ring = try linux.IoUring.init_params(std.math.ceilPowerOfTwo(u16, params.depth), &ring_params);

        // Use a larger sizes so we don't run out of capacity easily.
        const io_queue = try Queue(linux.io_uring_sqe).init(params.depth * 8, params.alloc);
        const io_results = try SliceMap(u64, i32).init(params.depth * 8, params.alloc);
        const io_submit_time = try SliceMap(u64, Instant).init(params.depth * 8, params.alloc);

        return Self{
            .ring = ring,
            .io_queue = io_queue,
            .io_results = io_results,
            .io_submit_time = io_submit_time,
            .pending_io = 0,
        };
    }

    pub fn deinit(self: Self, alloc: Allocator) void {
        std.debug.assert(self.pending_io == 0);
        self.io_queue.deinit(alloc);
        self.io_results.deinit(alloc);
        self.io_submit_time.deinit(alloc);
        self.ring.deinit();
    }

    /// Access io result map. Mapping of user_data -> io result code from kernel
    ///
    /// User can remove results from this map as needed.
    pub fn results(self: *Self) *SliceMap(u64, i32) {
        return &self.io_results;
    }

    fn maybe_warn_io_time(now: Instant, start: Instant) void {
        const io_time_threshold_ns = 10 * 1000 * 1000;
        const elapsed = now.since(start);
        if (elapsed > io_time_threshold_ns) {
            std.log.warn("an io command has been running for {}ms", .{elapsed / (1000 * 1000)});
        }
    }

    pub fn sync_cq(self: *Self) void {
        const ready = self.ring.cq_ready();
        const head = self.cq.head.* & self.cq.mask;

        const now = Instant.now() catch unreachable;

        // before wrapping
        const n = self.ring.cq.cqes.len - head;
        for (self.ring.cq.cqes[head .. head + @min(n, ready)]) |cqe| {
            const start = self.io_submit_time.remove(cqe.user_data) orelse unreachable;
            maybe_warn_io_time(now, start);

            std.debug.assert(self.io_results.insert(cqe.user_data, cqe.res) == null);
        }

        // wrap self.cq.cqes
        if (ready > n) {
            for (self.ring.cq.cqes[0 .. ready - n]) |cqe| {
                const start = self.io_submit_time.remove(cqe.user_data) orelse unreachable;
                maybe_warn_io_time(now, start);

                std.debug.assert(self.io_results.insert(cqe.user_data, cqe.res) == null);
            }
        }

        for (0..self.io_submit_time.values[0..self.io_submit_time.len]) |start| {
            maybe_warn_io_time(now, start);
        }

        self.cq_advance(ready);
        self.pending_io -= ready;
    }

    pub fn sync_sq(self: *Self) void {
        for (0..self.io_queue.length()) |_| {
            switch (self.ring.get_sqe()) {
                .ok => |sqe_ptr| {
                    self.pending_io += 1;
                    const sqe = self.io_queue.pop() orelse unreachable;
                    self.io_submit_time(sqe.user_data, Instant.now() catch unreachable) catch unreachable;
                    sqe_ptr.* = sqe;
                },
                .err => return,
            }
        }
    }
};
