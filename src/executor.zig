const std = @import("std");
const Instant = std.time.Instant;
const Allocator = std.mem.Allocator;
const linux = std.os.linux;

const Queue = @import("./queue.zig").Queue;
const SliceMap = @import("./slice_map.zig").SliceMap;
const Slab = @import("./slab.zig").Slab;
const Task = @import("./task.zig").Task;

pub const Executor = struct {
    const Self = @This();

    const TaskEntry = struct {
        task: Task,
        finished_io: [128]u64,
        num_finished_io: u8,
        num_pending_io: u8,
        finished_execution: bool,
    };

    io_ring: IoUring,
    polled_io_ring: IoUring,
    io: Slab(u64),
    tasks: Slab(TaskEntry),
    to_notify: SliceMap(u64, void),
    preempt_duration_ns: u64,

    pub fn init(params: struct {
        max_num_tasks: u16 = 1024,
        entries: u16 = 64,
        preempt_duration_ns: u64 = 10 * 1000 * 1000,
        wq_fd: ?linux.fd_t,
        alloc: Allocator,
    }) error{ OutOfMemory, IoUringSetupFail }!Self {
        const io_ring = try IoUring.init(.{
            .entries = params.entries,
            .wq_fd = params.wq_fd,
            .polled_io = false,
            .alloc = params.alloc,
        });

        const polled_io_ring = try IoUring.init(.{
            .entries = params.entries,
            .wq_fd = io_ring.ring.fd,
            .polled_io = true,
            .alloc = params.alloc,
        });

        const io = try Slab(u64).init(params.entries * 16, params.alloc);
        const tasks = try Slab(Task).init(params.max_num_tasks, params.alloc);
        const to_notify = try SliceMap(u64, void).init(params.max_num_tasks, params.alloc);

        return Self{
            .io_ring = io_ring,
            .polled_io_ring = polled_io_ring,
            .io = io,
            .tasks = tasks,
            .to_notify = to_notify,
            .preempt_duration_ns = params.preempt_duration_ns,
        };
    }

    pub fn deinit(self: *Self, alloc: Allocator) void {
        self.io_ring.deinit(alloc);
        self.polled_io_ring.deinit(alloc);
        self.io.deinit(alloc);
        self.tasks.deinit(alloc);
        self.to_notify.deinit(alloc);
    }

    pub fn run(self: *Self, main_task: Task) void {
        const main_task_id = self.tasks.insert(TaskEntry{
            .task = main_task,
            .finished_io = undefined,
            .num_finished_io = 0,
            .finished_execution = false,
        }) orelse unreachable;
        _ = self.to_notify.insert(main_task_id, {}) catch unreachable;

        while (!self.tasks.is_empty()) {
            if (self.to_notify.is_empty()) {
                while (self.io_ring.io_results.is_empty() and self.polled_io_ring.io_results.is_empty()) {
                    self.io_ring.sync_queues();
                    self.polled_io_ring.sync_queues();

                    // Don't hog CPU while waiting for io to finish or submission queue to free up.
                    // The sleep is for 1 nanosecond but the intention here is to just yield the cpu core to the OS so It can do other things with it
                    // before coming back to this thread.
                    std.Thread.sleep(1);
                }
            }

            for (self.to_notify.keys[0..self.to_notify.len]) |task_id| {
                if (self.tasks.get_mut_ref(task_id)) |entry| {
                    if (!entry.finished_execution) {
                        if (entry.task.poll(.{})) {
                            entry.finished_execution = true;
                        } else {
                            self.io_ring.sync_queues();
                            self.polled_io_ring.sync_queues();
                        }

                        // TODO: check if task took too much time and warn if it did
                    }
                }
            }

            self.io_ring.sync_queues();
            self.polled_io_ring.sync_queues();

            // TODO: process io_results of both
        }
    }
};

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

    fn init(params: struct {
        entries: u16,
        /// file descriptor of another ring, will be used to share background threads with the other ring if passed.
        wq_fd: ?linux.fd_t,
        /// If this argument is true then this ring can only be used for writing/reading from sockets(napi) and files(direct_io)
        polled_io: bool,
        /// Allocator to be used for allocating internal data structures, same allocator should be passed to the deinit method
        alloc: Allocator,
    }) error{ OutOfMemory, IoUringSetupFail }!Self {
        std.debug.assert(params.entries > 0);

        var flags: u32 = linux.IORING_SETUP_SQPOLL | linux.IORING_SETUP_COOP_TASKRUN | linux.IORING_SETUP_SINGLE_ISSUER;

        if (params.polled_io) {
            flags |= linux.IORING_SETUP_IOPOLL;
        }

        if (params.wq_fd != null) {
            flags |= linux.IORING_SETUP_ATTACH_WQ;
        }

        var ring_params = std.mem.zeroInit(linux.io_uring_params, .{
            .wq_fd = if (params.wq_fd) |fd| @intCast(fd) else 0,
            .sq_thread_idle = 1000,
            .flags = flags,
        });
        const ring = linux.IoUring.init_params(std.math.ceilPowerOfTwo(u16, params.entries), &ring_params) catch {
            return error.IoUringSetupFail;
        };

        // Use a larger sizes so we don't run out of capacity easily.
        const io_queue = try Queue(linux.io_uring_sqe).init(params.entries * 8, params.alloc);
        const io_results = try SliceMap(u64, i32).init(params.entries * 8, params.alloc);
        const io_submit_time = try SliceMap(u64, Instant).init(params.entries * 8, params.alloc);

        return Self{
            .ring = ring,
            .io_queue = io_queue,
            .io_results = io_results,
            .io_submit_time = io_submit_time,
            .pending_io = 0,
        };
    }

    fn deinit(self: Self, alloc: Allocator) void {
        std.debug.assert(self.pending_io == 0);
        self.ring.deinit();
        self.io_queue.deinit(alloc);
        self.io_results.deinit(alloc);
        self.io_submit_time.deinit(alloc);
    }

    fn maybe_warn_io_time(now: Instant, start: Instant) void {
        const io_time_threshold_ns = 10 * 1000 * 1000;
        const elapsed = now.since(start);
        if (elapsed > io_time_threshold_ns) {
            std.log.warn("an io command has been running for {}ms", .{elapsed / (1000 * 1000)});
        }
    }

    fn sync_cq(self: *Self) void {
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

        self.ring.cq_advance(ready);
        self.pending_io -= ready;
    }

    fn sync_sq(self: *Self) void {
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

    fn sync_queues(self: *Self) void {
        self.sync_cq();
        self.sync_sq();
    }
};
