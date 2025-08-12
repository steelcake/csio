const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn Slab(comptime T: type) type {
    const Entry = union(enum) {
        occupied: T,
        // index of the next free slot. there is no space left if this index is equal to
        // the number of slots
        free: u32,
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

        pub fn insert(self: *Self, val: T) error{OutOfCapacity}!u32 {
            const key_idx = self.first_free_entry;
            if (key_idx == self.elems.len) {
                return error.OutOfCapacity;
            }

            const entry = self.elems[key_idx];

            switch (entry) {
                .occupied => unreachable,
                .free => |next_free_e| {
                    self.first_free_entry = next_free_e;
                },
            }

            self.elems[key_idx] = Entry{
                .occupied = val,
            };

            return key_idx;
        }

        pub fn get(self: *const Self, key: u32) ?*const T {
            const entry = &self.elems[key];
            return switch (entry.*) {
                .occupied => |*val| val,
                .free => null,
            };
        }

        pub fn get_mut(self: *Self, key: u32) ?*T {
            const entry = &self.elems[key];
            return switch (entry.*) {
                .occupied => |*val| val,
                .free => null,
            };
        }

        pub fn remove(self: *Self, key: u32) ?T {
            const val = switch (self.elems[key]) {
                .occupied => |val| val,
                .free => return null,
            };

            self.elems[key] = .{ .free = self.first_free_entry };
            self.first_free_entry = key;

            return val;
        }
    };
}
