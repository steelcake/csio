const std = @import("std");
const HashMap = std.AutoHashMap;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const SliceMap = @import("./slice_map.zig").SliceMap;
const FuzzInput = @import("./fuzz_input.zig").FuzzInput;

fn to_fuzz(data: []const u8) !void {
    var input = FuzzInput.init(data);

    var hashmap = HashMap(i8, u8).init(std.heap.page_allocator);
    defer hashmap.deinit();

    const capacity = try input.int(u8);
    try hashmap.ensureTotalCapacity(capacity);

    var slicemap = try SliceMap(i8, u8).init(capacity, std.heap.page_allocator);
    defer slicemap.deinit(std.heap.page_allocator);

    while (true) {
        switch ((try input.int(u8)) % 5) {
            // insert
            0 => {
                const key = try input.int(i8);
                const value = try input.int(u8);

                const existing = slicemap.insert(key, value) catch |e| {
                    if (e == error.OutOfCapacity) return else unreachable;
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

fn to_fuzz_wrap(_: void, data: []const u8) anyerror!void {
    return to_fuzz(data) catch |e| {
        if (e == error.ShortInput) return {} else return e;
    };
}

test "fuzz" {
    try std.testing.fuzz({}, to_fuzz_wrap, .{});
}
