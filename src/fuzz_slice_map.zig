const std = @import("std");
const HashMap = std.AutoHashMap;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const SliceMap = @import("./slice_map.zig").SliceMap;
const FuzzInput = @import("./fuzz_input.zig").FuzzInput;

fn to_fuzz(data: []const u8, alloc: Allocator) !void {
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

const FuzzContext = struct {
    arena: *ArenaAllocator,
};

fn to_fuzz_wrap(ctx: FuzzContext, data: []const u8) anyerror!void {
    std.debug.assert(ctx.arena.reset(.retain_capacity));
    return to_fuzz(data, ctx.arena.allocator()) catch |e| {
        if (e == error.ShortInput) return {} else return e;
    };
}

test "fuzz" {
    var arena = ArenaAllocator.init(std.heap.page_allocator);
    try std.testing.fuzz(FuzzContext{
        .arena = &arena,
    }, to_fuzz_wrap, .{});
}
