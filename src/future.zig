const std = @import("std");
const Instant = std.time.Instant;

const Slab = @import("./slab.zig").Slab;
const SliceMap = @import("./slice_map.zig").SliceMap;

pub const Future = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
};

pub const Context = struct {
    start_t: Instant,
    task_id: u32,
    tasks: *Slab(Future),
    io_results: *SliceMap(u64, i32),
};

pub const VTable = struct {
    poll: *const fn (*anyopaque, ctx: *Context) void,
};
