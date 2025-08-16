const std = @import("std");
const Instant = std.time.Instant;
const linux = std.os.linux;

const slab_mod = @import("./slab.zig");
const Slab = slab_mod.Slab;
const SlabKey = slab_mod.Key;

const SliceMap = @import("./slice_map.zig").SliceMap;
const Queue = @import("./queue.zig").Queue;

pub const MAX_IO_PER_TASK = 128;

pub const TaskEntry = struct {
    task: Task,
    finished_io_id: [MAX_IO_PER_TASK]u64,
    finished_io_result: [MAX_IO_PER_TASK]i32,
    num_finished_io: u8,
    num_pending_io: u8,
    finished_execution: bool,
};

pub const Context = struct {
    start_t: Instant,
    task_id: u32,
    task_entry: *TaskEntry,
    io_queue: *Queue(linux.io_uring_sqe),
    polled_io_queue: *Queue(linux.io_uring_sqe),
    io: *Slab(SlabKey),
    to_notify: *SliceMap(SlabKey, void),
    preempt_duration_ns: u64,
};

pub const Task = struct {
    ptr: *anyopaque,
    poll_fn: *const fn (*anyopaque, ctx: *Context) void,

    pub fn poll(self: Task, ctx: *Context) void {
        self.poll_fn(self.ptr, ctx);
    }
};
