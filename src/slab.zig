const std = @import("std");
const Allocator = std.mem.Allocator;

const Key = struct {
    generation: u32,
    index: u32,
};

pub fn Slab(comptime T: type) type {
    const Entry = union(enum) {
        occupied: struct { generation: u32, val: T },
        free: struct { next_free: u32 },
    };

    return struct {
        const Self = @This();

        entries: []Entry,
        // index of the first free slot. there is no space left if this index is equal to
        // the number of slots
        first_free_entry: u32,
        current_generation: u32,

        pub fn init(capacity: u32, alloc: Allocator) error{OutOfMemory}!Self {
            const entries = try alloc.alloc(Entry, capacity);

            var idx: u32 = 0;
            while (idx < capacity) : (idx += 1) {
                entries[idx] = .{ .free = idx + 1 };
            }

            return Self{
                .entries = entries,
                .first_free_entry = 0,
                .current_generation = 0,
            };
        }

        pub fn deinit(self: Self, alloc: Allocator) void {
            alloc.free(self.entries);
        }

        pub fn insert(self: *Self, val: T) error{OutOfCapacity}!Key {
            const key_idx = self.first_free_entry;
            if (key_idx == self.entries.len) {
                return error.OutOfCapacity;
            }

            const entry = self.entries[key_idx];

            switch (entry) {
                .occupied => unreachable,
                .free => |f| {
                    self.first_free_entry = f.next_free;
                },
            }

            self.entries[key_idx] = Entry{
                .occupied = val,
            };

            return Key{ .index = key_idx, .generation = self.current_generation };
        }

        pub fn get(self: *const Self, key: Key) ?T {
            return switch (self.entries[key.index]) {
                .occupied => |entry| {
                    if (entry.generation != key.generation) {
                        return null;
                    }

                    return entry.val;
                },
                .free => null,
            };
        }

        pub fn remove(self: *Self, key: Key) ?T {
            switch (self.entries[key.index]) {
                .occupied => |entry| {
                    if (entry.generation != key.generation) {
                        return null;
                    }

                    self.entries[key.index] = .{ .free = .{ .next_free = self.first_free_entry } };
                    self.first_free_entry = key.index;
                    self.current_generation +%= 1;

                    return entry.val;
                },
                .free => return null,
            }
        }
    };
}
