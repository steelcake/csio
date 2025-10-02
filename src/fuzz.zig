const std = @import("std");
const FixedBufferAllocator = std.heap.FixedBufferAllocator;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const HashMap = std.AutoHashMap;

const SliceMap = @import("./slice_map.zig").SliceMap;
const Slab = @import("./slab.zig").Slab;
const Queue = @import("./queue.zig").Queue;
const IoAlloc = @import("./io_alloc.zig").IoAlloc;

const FuzzInput = @import("./fuzz_input.zig").FuzzInput;

// check if two slices overlap
fn overlaps(a: []const u8, b: []const u8) bool {
    const a_start_addr = @intFromPtr(a.ptr);
    const a_end_addr = a_start_addr + a.len;
    const b_start_addr = @intFromPtr(b.ptr);
    const b_end_addr = b_start_addr + b.len;

    return (a_start_addr >= b_start_addr and a_start_addr < b_end_addr) or (b_start_addr >= a_start_addr and b_start_addr < a_end_addr);
}

fn fuzz_io_alloc(data: []const u8, gpa: Allocator) !void {
    const MAX_ALLOC = 1 << 27;

    std.debug.assert(MAX_ALLOC % IoAlloc.ALIGN == 0);

    const StaticBackingBuf = struct {
        var buf: ?[]align(512) u8 = null;

        fn get_buf() []align(512) u8 {
            if (buf) |b| {
                return b;
            } else {
                const b = std.heap.page_allocator.alignedAlloc(u8, std.mem.Alignment.fromByteUnits(512), MAX_ALLOC) catch unreachable;
                buf = b;
                return b;
            }
        }
    };

    var input = FuzzInput.init(data);

    const backing_buf = StaticBackingBuf.get_buf();

    const max_num_slots = backing_buf.len / IoAlloc.ALIGN;
    const num_slots = (try input.int(u8)) % max_num_slots;
    const capacity: u32 = @intCast(num_slots * IoAlloc.ALIGN);

    var arena = ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    var io_alloc = try IoAlloc.init(backing_buf[0..capacity], alloc);
    defer io_alloc.deinit(alloc);

    const buffers = try alloc.alloc([]align(IoAlloc.ALIGN) u8, num_slots);
    defer alloc.free(buffers);
    var num_buffers: u32 = 0;

    defer for (buffers[0..num_buffers]) |buf| {
        io_alloc.free(buf);
    };

    while (true) {
        switch ((try input.int(u8)) % 2) {
            // alloc
            0 => {
                if (num_buffers == num_slots) {
                    continue;
                }

                const size = (try input.int(u16)) % capacity;
                const aligned_size = (size + IoAlloc.ALIGN - 1) / IoAlloc.ALIGN * IoAlloc.ALIGN;

                const buf = io_alloc.alloc(aligned_size) catch continue;

                buffers[num_buffers] = buf;
                num_buffers += 1;
            },
            // free
            1 => {
                if (num_buffers == 0) {
                    continue;
                }

                const index = (try input.int(u8)) % num_buffers;
                const buf = buffers[index];
                num_buffers -= 1;
                buffers[index] = buffers[num_buffers];

                io_alloc.free(buf);
            },
            else => unreachable,
        }

        for (buffers[0..num_buffers], 0..) |a, idx| {
            for (buffers[idx + 1 .. num_buffers]) |b| {
                std.debug.assert(!overlaps(a, b));
            }
        }
    }
}

test "fuzz io_alloc" {
    try FuzzWrap(fuzz_io_alloc, 1 << 20).run();
}

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

    while (pop_index < values.len or push_index < values.len) {
        std.debug.assert(push_index <= values.len);
        std.debug.assert(pop_index <= values.len);

        switch ((try input.int(u8)) % 2) {
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
            // pop
            1 => {
                if (pop_index == values.len) {
                    std.debug.assert(queue.len == 0 and queue.pop() == null and queue.is_empty());
                    continue;
                }

                // the queue should be empty
                if (pop_index == push_index) {
                    std.debug.assert(queue.len == 0 and queue.pop() == null and queue.is_empty());
                    continue;
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

test "fuzz queue" {
    try FuzzWrap(fuzz_queue, 1 << 20).run();
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

                const slab_value = slab.remove(key) orelse unreachable;
                const map_value = map.remove(key) orelse unreachable;
                std.debug.assert(map_value == slab_value);
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

test "fuzz slab" {
    try FuzzWrap(fuzz_slab, 1 << 20).run();
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
                        break if (slicemap.swap_remove(idx)) |kv| kv.value else null;
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

test "fuzz slicemap" {
    try FuzzWrap(fuzz_slicemap, 1 << 20).run();
}

fn FuzzWrap(comptime fuzz_one: fn (data: []const u8, gpa: Allocator) anyerror!void, comptime alloc_size: comptime_int) type {
    const FuzzContext = struct {
        fb_alloc: *FixedBufferAllocator,
    };

    return struct {
        fn run_one(ctx: FuzzContext, data: []const u8) anyerror!void {
            ctx.fb_alloc.reset();

            var general_purpose_allocator = std.heap.GeneralPurposeAllocator(.{
                .backing_allocator_zeroes = false,
            }){
                .backing_allocator = ctx.fb_alloc.allocator(),
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

        fn run() !void {
            var fb_alloc = FixedBufferAllocator.init(std.heap.page_allocator.alloc(u8, alloc_size) catch unreachable);
            try std.testing.fuzz(FuzzContext{
                .fb_alloc = &fb_alloc,
            }, run_one, .{});
        }
    };
}
