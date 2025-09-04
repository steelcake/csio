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

    preempt_duration_ns: u64,
    io_alloc: *IoAlloc,

    pub fn yield_if_needed(self: *const Context) bool {
        const now = Instant.now() catch unreachable;

        if (now.since(self.start_t) > self.preempt_duration_ns) {
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
