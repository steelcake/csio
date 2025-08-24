const std = @import("std");
const FixedBufferAllocator = std.heap.FixedBufferAllocator;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const HashMap = std.AutoHashMap;

const SliceMap = @import("./slice_map.zig").SliceMap;
const Slab = @import("./slab.zig").Slab;
const Queue = @import("./queue.zig").Queue;

const FuzzInput = @import("./fuzz_input.zig").FuzzInput;

fn fuzz_queue(data: []const u8, alloc: Allocator) !void {
    var input = FuzzInput.init(data);

    const num_values = try input.int(u8);
    const values = try input.slice(i8, num_values, alloc);
    defer alloc.free(values);

    var pop_index: usize = 0;
    var push_index: usize = 0;

    const capacity = try input.int(u8);

    var queue = try Queue(i8).init(capacity, alloc);
    defer queue.deinit(alloc);

    while (true) {
        std.debug.assert(push_index <= values.len);
        std.debug.assert(pop_index <= values.len);

        switch ((try input.int(u8)) % 4) {
            // push
            0 => {
                if (push_index == values.len) {
                    continue;
                }

                const value = values[push_index];

                queue.push(value) catch |e| {
                    if (e == error.OutOfCapacity) continue else unreachable;
                };

                push_index += 1;
            },
            // push_batch
            2 => {
                if (push_index == values.len) {
                    continue;
                }
                const left_capacity = values.len - push_index;

                const batch_size = (try input.int(u8)) % left_capacity;

                const batch = values[push_index .. push_index + batch_size];

                queue.push_batch(batch) catch unreachable;

                push_index += batch_size;
            },
            // pop
            3 => {
                if (pop_index == values.len) {
                    continue;
                }

                // the queue should be empty
                if (pop_index == push_index) {
                    std.debug.assert(queue.pop() == null);
                }

                const value = values[pop_index];

                const pop_val = queue.pop() orelse unreachable;

                std.debug.assert(value == pop_val);

                pop_index += 1;
            },
            else => unreachable,
        }
    }
}

fn fuzz_slab(data: []const u8, alloc: Allocator) !void {
    var input = FuzzInput.init(data);

    const capacity = try input.int(u8);

    var map = try SliceMap(u64, i8).init(capacity, alloc);
    defer map.deinit(alloc);

    var slab = try Slab(i8).init(capacity, alloc);
    defer slab.deinit(alloc);

    while (true) {
        switch ((try input.int(u8)) % 4) {
            // insert
            0 => {
                const value = try input.int(i8);
                const key = slab.insert(value) catch |e| {
                    if (e == error.OutOfCapacity) continue else unreachable;
                };
                std.debug.assert((map.insert(key, value) catch unreachable) == null);
            },
            // get
            1 => {
                if (map.len == 0) continue;
                const index = (try input.int(u8)) % map.len;
                const key = map.keys[index];
                const value = map.values[index];

                const slab_value = slab.get(key) orelse unreachable;
                std.debug.assert(value == slab_value);

                const slab_value_mut = slab.get_mut_ref(key) orelse unreachable;
                std.debug.assert(value == slab_value_mut.*);
            },
            // remove
            2 => {
                if (map.len == 0) continue;
                const index = (try input.int(u8)) % map.len;
                const key = map.keys[index];
                const value = map.values[index];

                const slab_value = slab.remove(key) orelse unreachable;
                std.debug.assert(value == slab_value);
            },
            // modify in-place
            3 => {
                if (map.len == 0) continue;
                const index = (try input.int(u8)) % map.len;
                const value = try input.int(i8);

                const key = map.keys[index];

                const slab_ref = slab.get_mut_ref(key) orelse unreachable;

                std.debug.assert(slab_ref.* == map.values[index]);

                slab_ref.* = value;
                map.values[index] = value;
            },
            else => unreachable,
        }
    }
}

fn fuzz_slicemap(data: []const u8, alloc: Allocator) !void {
    var input = FuzzInput.init(data);

    var hashmap = HashMap(i8, u8).init(alloc);
    defer hashmap.deinit();

    const capacity = try input.int(u8);
    try hashmap.ensureTotalCapacity(capacity);

    var slicemap = try SliceMap(i8, u8).init(capacity, alloc);
    defer slicemap.deinit(alloc);

    while (true) {
        switch ((try input.int(u8)) % 5) {
            // insert
            0 => {
                const key = try input.int(i8);
                const value = try input.int(u8);

                const existing = slicemap.insert(key, value) catch |e| {
                    if (e == error.OutOfCapacity) continue else unreachable;
                };
                const ref_existing = if (hashmap.fetchPut(key, value) catch unreachable) |kv| kv.value else null;
                std.debug.assert(existing == ref_existing);
            },
            // get
            1 => {
                const key = try input.int(i8);
                const val = slicemap.get(key);
                const ref_val = hashmap.get(key);
                std.debug.assert(val == ref_val);
            },
            // remove
            2 => {
                const key = try input.int(i8);
                const val = slicemap.remove(key);
                const ref_val = if (hashmap.fetchRemove(key)) |kv| kv.value else null;
                std.debug.assert(val == ref_val);
            },
            // swap remove
            3 => {
                const key = try input.int(i8);

                var idx: u32 = 0;
                const val = while (idx < slicemap.len) : (idx += 1) {
                    if (slicemap.keys[idx] == key) {
                        break slicemap.swap_remove(idx);
                    }
                } else null;
                const ref_val = if (hashmap.fetchRemove(key)) |kv| kv.value else null;
                std.debug.assert(val == ref_val);
            },
            // compare
            4 => {
                std.debug.assert(slicemap.len == hashmap.count());
                var iter = hashmap.iterator();
                while (iter.next()) |kv| {
                    std.debug.assert(slicemap.get(kv.key_ptr.*) == kv.value_ptr.*);
                }
            },
            else => unreachable,
        }
    }
}

const FuzzContext = struct {
    fb_alloc: *FixedBufferAllocator,
};

fn run_fuzz_test(comptime fuzz_one: fn (data: []const u8, gpa: Allocator) anyerror!void, data: []const u8, fb_alloc: *FixedBufferAllocator) anyerror!void {
    fb_alloc.reset();

    var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{
        .backing_allocator_zeroes = false,
    }){
        .backing_allocator = fb_alloc.allocator(),
    };
    const gpa = general_purpose_allocator.allocator();
    defer {
        switch (general_purpose_allocator.deinit()) {
            .ok => {},
            .leak => |l| {
                std.debug.panic("LEAK: {any}", .{l});
            },
        }
    }

    fuzz_one(data, gpa) catch |e| {
        if (e == error.ShortInput) return {} else return e;
    };
}

fn to_fuzz_wrap(ctx: FuzzContext, data: []const u8) anyerror!void {
    try run_fuzz_test(fuzz_slicemap, data, ctx.fb_alloc);
    try run_fuzz_test(fuzz_slab, data, ctx.fb_alloc);
    try run_fuzz_test(fuzz_queue, data, ctx.fb_alloc);
}

test "fuzz" {
    var fb_alloc = FixedBufferAllocator.init(std.heap.page_allocator.alloc(u8, 1 << 20) catch unreachable);
    try std.testing.fuzz(FuzzContext{
        .fb_alloc = &fb_alloc,
    }, to_fuzz_wrap, .{});
}
