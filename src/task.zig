const std = @import("std");
const Instant = std.time.Instant;
const linux = std.os.linux;

const slab_mod = @import("./slab.zig");
const Slab = slab_mod.Slab;
const SlabKey = slab_mod.Key;

const SliceMap = @import("./slice_map.zig").SliceMap;
const Queue = @import("./queue.zig").Queue;

pub const Task = struct {
    pub const VTable = struct {
        poll: *const fn (*anyopaque, ctx: *Context) bool,
        deinit: *const fn (*anyopaque) void,
    };

    ptr: *anyopaque,
    vtable: VTable,

    pub fn poll(self: Task, ctx: *Context) bool {
        return self.vtable.poll(self.ptr, ctx);
    }

    pub fn deinit(self: Task) void {
        return self.vtable.deinit(self.ptr);
    }
};

pub const Context = struct {
    start_t: Instant,
    task_id: u32,
    tasks: *Slab(Task),
    io_results: *SliceMap(SlabKey, i32),
    io_queue: *Queue(linux.io_uring_sqe),
    dio_queue: *Queue(linux.io_uring_sqe),
    preempt_duration_ns: u64,
    io: *Slab(SlabKey),
    to_notify: *SliceMap(SlabKey, void),
    notify_when: *SliceMap(),
};
