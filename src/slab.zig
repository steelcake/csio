const std = @import("std");
const Allocator = std.mem.Allocator;

const Key = packed struct(u64) {
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
        n_occupied: u32,

        pub fn init(capacity: u32, alloc: Allocator) error{OutOfMemory}!Self {
            const entries = try alloc.alloc(Entry, capacity);

            var idx: u32 = 0;
            while (idx < capacity) : (idx += 1) {
                entries[idx] = .{ .free = .{ .next_free = idx + 1 } };
            }

            return Self{
                .entries = entries,
                .first_free_entry = 0,
                .current_generation = 0,
                .n_occupied = 0,
            };
        }

        pub fn deinit(self: Self, alloc: Allocator) void {
            alloc.free(self.entries);
        }

        pub fn num_occupied(self: *const Self) u32 {
            return self.n_occupied;
        }

        pub fn is_empty(self: *const Self) bool {
            return self.n_occupied == 0;
        }

        pub fn insert(self: *Self, val: T) error{OutOfCapacity}!u64 {
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
                .occupied = .{ .generation = self.current_generation, .val = val },
            };

            self.n_occupied += 1;

            return @bitCast(Key{ .index = key_idx, .generation = self.current_generation });
        }

        pub fn get(self: *const Self, key: u64) ?T {
            const k: Key = @bitCast(key);
            return switch (self.entries[k.index]) {
                .occupied => |entry| {
                    if (entry.generation != k.generation) {
                        return null;
                    }

                    return entry.val;
                },
                .free => null,
            };
        }

        pub fn get_mut_ref(self: *const Self, key: u64) ?*T {
            const k: Key = @bitCast(key);
            return switch (self.entries[k.index]) {
                .occupied => |*entry| {
                    if (entry.generation != k.generation) {
                        return null;
                    }

                    return &entry.val;
                },
                .free => null,
            };
        }

        pub fn remove(self: *Self, key: u64) ?T {
            const k: Key = @bitCast(key);
            switch (self.entries[k.index]) {
                .occupied => |entry| {
                    if (entry.generation != k.generation) {
                        return null;
                    }

                    self.entries[k.index] = .{ .free = .{ .next_free = self.first_free_entry } };
                    self.first_free_entry = k.index;
                    self.current_generation +%= 1;
                    self.n_occupied -= 1;

                    return entry.val;
                },
                .free => return null,
            }
        }
    };
}
