// const std = @import("std");
// const HashMap = std.HashMapUnmanaged;
// const Allocator = std.mem.Allocator;
// const ArenaAllocator = std.heap.ArenaAllocator;

// const SliceMap = @import("./slice_map.zig").SliceMap;

// fn to_fuzz(_: void, data: []const u8) anyerror!void {
//     if (data.len == 0) return;

//     var hashmap = HashMap().init();
//     defer hashmap.deinit();

//     var slicemap = SliceMap().init();
//     defer slicemap.deinit();

//     switch (data[0]) {
//         0 => {
//             const key = ;
//         },
//         else => unreachable,
//     }
// }

// test "fuzz" {
//     try std.testing.fuzz({}, to_fuzz, .{});
// }
