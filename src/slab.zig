const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn Slab(comptime T: type) type {
    const Key = packed struct(u64) {
        generation: u32,
        index: u32,
    };

    const Entry = union(enum) {
        occupied: struct { generation: u32, val: T },
        // index of the next free slot. there is no space left if this index is equal to
        // the number of slots
        free: struct { next_free: u32 },
    };

    return struct {
        const Self = @This();

        elems: []Entry,
        first_free_entry: u32,
        current_generation: u32,

        pub fn init(capacity: u32, alloc: Allocator) error{OutOfMemory}!Self {
            const elems = try alloc.alloc(Entry, capacity);

            var idx: u32 = 0;
            while (idx < capacity) : (idx += 1) {
                elems[idx] = .{ .free = idx + 1 };
            }

            return Self{
                .elems = elems,
                .first_free_entry = 0,
                .current_generation = 0,
            };
        }

        pub fn deinit(self: Self, alloc: Allocator) void {
            alloc.free(self.elems);
        }

        pub fn insert(self: *Self, val: T) error{OutOfCapacity}!u64 {
            const key_idx = self.first_free_entry;
            if (key_idx == self.elems.len) {
                return error.OutOfCapacity;
            }

            const entry = self.elems[key_idx];

            switch (entry) {
                .occupied => unreachable,
                .free => |f| {
                    self.first_free_entry = f.next_free;
                },
            }

            self.elems[key_idx] = Entry{
                .occupied = val,
            };

            return Key{ .index = key_idx, .generation = self.current_generation };
        }

        pub fn get(self: *const Self, key: u64) ?T {
            const k = @as(Key, @bitCast(key));
            return switch (self.elems[k.index]) {
                .occupied => |entry| {
                    if (entry.generation != k.generation) {
                        return null;
                    }

                    return entry.val;
                },
                .free => null,
            };
        }

        pub fn remove(self: *Self, key: u64) ?T {
            const k = @as(Key, @bitCast(key));
            switch (self.elems[k.index]) {
                .occupied => |entry| {
                    if (entry.generation != k.generation) {
                        return null;
                    }

                    self.elems[k.index] = .{ .free = .{ .next_free = self.first_free_entry } };
                    self.first_free_entry = k.index;
                    self.current_generation +%= 1;

                    return entry.val;
                },
                .free => return null,
            }
        }
    };
}
