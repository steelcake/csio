const std = @import("std");
const Instant = std.time.Instant;
const linux = std.os.linux;

const slab_mod = @import("./slab.zig");
const Slab = slab_mod.Slab;
const IoAlloc = @import("./io_alloc.zig").IoAlloc;
const SliceMap = @import("./slice_map.zig").SliceMap;
const Queue = @import("./queue.zig").Queue;
const IoUring = @import("./executor.zig").IoUring;

pub const MAX_IO_PER_TASK = 128;

pub const TaskEntry = struct {
    task: Task,
    finished_io: [MAX_IO_PER_TASK]linux.io_uring_cqe,
    num_finished_io: u8,
    num_pending_io: u8,

    pub fn init(task: Task) TaskEntry {
        return .{
            .task = task,
            .finished_io = undefined,
            .num_finished_io = 0,
            .num_pending_io = 0,
        };
    }
};

pub const Context = struct {
    start_t: Instant,
    task_id: u64,
    task_entry: *TaskEntry,
    ring: *IoUring,
    polled_ring: *IoUring,

    // io_id -> task_id
    io: *Slab(u64),

    // task_id -> void
    to_notify: *SliceMap(u64, void),

    fixed_fd: []linux.fd_t,

    preempt_duration_ns: u64,
    direct_io_alloc: *IoAlloc,

    tasks: Slab(TaskEntry),

    pub fn spawn(self: *const Context, task: Task) void {
        const task_id = self.tasks.insert(TaskEntry.init(task)) catch unreachable;
        _ = self.to_notify.insert(task_id, {}) catch unreachable;
    }

    pub fn yield_if_needed(self: *const Context) bool {
        const now = Instant.now() catch unreachable;

        // leave 500us difference here
        if (now.since(self.start_t) >= self.preempt_duration_ns - 500000) {
            _ = self.to_notify.insert(self.task_id, {}) catch unreachable;
            return true;
        } else {
            return false;
        }
    }

    pub fn notify_self(self: *const Context) void {
        _ = self.to_notify.insert(self.task_id, {}) catch unreachable;
    }

    /// For queueing direct_io read/write only
    pub fn queue_polled_io(self: *const Context, io: linux.io_uring_sqe) u64 {
        return self.queue_io_impl(true, io);
    }

    pub fn queue_io(self: *const Context, io: linux.io_uring_sqe) u64 {
        return self.queue_io_impl(false, io);
    }

    fn queue_io_impl(self: *const Context, polled: bool, io: linux.io_uring_sqe) u64 {
        const entry = self.task_entry;

        std.debug.assert(entry.num_pending_io + entry.num_finished_io < MAX_IO_PER_TASK);

        entry.num_pending_io += 1;

        var sqe = io;
        const io_id = self.io.insert(self.task_id) catch unreachable;
        sqe.user_data = io_id;

        if (polled) {
            self.polled_ring.queue_io(sqe);
        } else {
            self.ring.queue_io(sqe);
        }

        return io_id;
    }

    pub fn remove_io_result(self: *const Context, io_id: u64) ?linux.io_uring_cqe {
        for (self.task_entry.finished_io[0..self.task_entry.num_finished_io], 0..) |cqe, idx| {
            if (cqe.user_data == io_id) {
                const task_id = self.io.remove(io_id) orelse unreachable;
                self.task_entry.num_finished_io -= 1;
                self.task_entry.finished_io[idx] = self.task_entry.finished_io[self.task_entry.num_finished_io];
                std.debug.assert(task_id == self.task_id);
                return cqe;
            }
        }

        return null;
    }

    pub fn alloc_direct_io_buf(self: *const Context, size: u32) []align(512) u8 {
        return self.direct_io_alloc.alloc(size) catch unreachable;
    }

    pub fn free_direct_io_buf(self: *const Context, buf: []align(512) u8) void {
        self.direct_io_alloc.free(buf);
    }

    pub fn register_fd(self: *const Context, fd: linux.fd_t) u32 {
        std.debug.assert(fd > -1);

        for (self.fixed_fd) |f| {
            if (f == fd) {
                unreachable;
            }
        }

        const idx = for (self.fixed_fd, 0..) |f, idx| {
            if (f == -1) {
                self.fixed_fd[idx] = fd;
                break @as(u32, @intCast(idx));
            }
        } else unreachable;

        self.ring.ring.register_files_update(idx, &.{fd}) catch unreachable;
        self.polled_ring.ring.register_files_update(idx, &.{fd}) catch unreachable;

        return idx;
    }

    pub fn unregister_fd(self: *const Context, idx: u32) linux.fd_t {
        const fd = self.fixed_fd[idx];
        std.debug.assert(fd > -1);
        self.ring.ring.register_files_update(idx, &.{-1}) catch unreachable;
        self.polled_ring.ring.register_files_update(idx, &.{-1}) catch unreachable;
        self.fixed_fd[idx] = -1;
        return fd;
    }
};

pub const IoOp = struct {
    const Self = @This();

    sqe: linux.io_uring_sqe,
    io_id: ?u64,
    finished: bool,
    is_polled: bool,

    pub fn init_polled(sqe: linux.io_uring_sqe) Self {
        return .{
            .sqe = sqe,
            .io_id = null,
            .finished = false,
            .is_polled = true,
        };
    }

    pub fn init(sqe: linux.io_uring_sqe) Self {
        return .{
            .sqe = sqe,
            .io_id = null,
            .finished = false,
            .is_polled = false,
        };
    }

    pub fn poll(self: *Self, ctx: *const Context) Poll(Result(i32, linux.E)) {
        std.debug.assert(!self.finished);

        if (self.io_id) |io_id| {
            if (ctx.remove_io_result(io_id)) |cqe| {
                self.finished = true;
                switch (cqe.err()) {
                    .SUCCESS => {
                        return .{ .ready = .{ .ok = cqe.res } };
                    },
                    else => |e| return .{ .ready = .{ .err = e } },
                }
            } else {
                return .pending;
            }
        } else {
            self.io_id = if (self.is_polled) ctx.queue_polled_io(self.sqe) else ctx.queue_io(self.sqe);
            return .pending;
        }
    }
};

pub const Fd = union(enum) {
    fixed: u32,
    fd: linux.fd_t,
};

pub fn Result(comptime T: type, comptime E: type) type {
    return union(enum) {
        ok: T,
        err: E,
    };
}

pub fn Poll(comptime T: type) type {
    return union(enum) {
        ready: T,
        pending,
    };
}

pub const Task = struct {
    ptr: *anyopaque,
    poll_fn: *const fn (*anyopaque, ctx: *const Context) Poll(void),

    pub fn poll(self: Task, ctx: *const Context) Poll(void) {
        return self.poll_fn(self.ptr, ctx);
    }
};

pub const Close = struct {
    op: IoOp,

    pub fn init(fd: linux.fd_t) Close {
        var sqe = std.mem.zeroes(linux.io_uring_sqe);
        sqe.prep_close(fd);
        return .{
            .op = IoOp.init(sqe),
        };
    }

    pub fn poll(self: *Close, ctx: *const Context) Poll(Result(void, linux.E)) {
        switch (self.op.poll(ctx)) {
            .ready => |res| {
                switch (res) {
                    .ok => |r| {
                        std.debug.assert(r == 0);
                        return .{ .ready = .{ .ok = {} } };
                    },
                    .err => |e| {
                        return .{ .ready = .{ .err = e } };
                    },
                }
            },
            .pending => return .pending,
        }
    }
};
