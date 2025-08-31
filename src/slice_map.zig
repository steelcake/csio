const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn SliceMap(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();

        keys: []K,
        values: []V,
        len: u32,

        pub fn init(capacity: u32, alloc: Allocator) error{OutOfMemory}!Self {
            const keys = try alloc.alloc(K, capacity);
            const values = try alloc.alloc(V, capacity);

            return Self{
                .keys = keys,
                .values = values,
                .len = 0,
            };
        }

        pub fn deinit(self: Self, alloc: Allocator) void {
            alloc.free(self.keys);
            alloc.free(self.values);
        }

        pub fn swap_remove(self: *Self, index: u32) ?struct { key: K, value: V } {
            if (index >= self.len) {
                return null;
            }

            // useless but for clarity
            std.debug.assert(self.len > 0);

            const out = .{ .value = self.values[index], .key = self.keys[index] };

            const end = self.len - 1;
            self.keys[index] = self.keys[end];
            self.values[index] = self.values[end];
            self.len -= 1;

            return out;
        }

        pub fn is_empty(self: *const Self) bool {
            return self.len == 0;
        }

        pub fn insert(self: *Self, key: K, value: V) error{OutOfCapacity}!?V {
            var idx: u32 = 0;
            while (idx < self.len) : (idx += 1) {
                if (self.keys[idx] == key) {
                    const old_value = self.values[idx];
                    self.values[idx] = value;
                    return old_value;
                }
            }

            if (self.len == self.keys.len) {
                return error.OutOfCapacity;
            }

            self.keys[self.len] = key;
            self.values[self.len] = value;
            self.len += 1;
            return null;
        }

        pub fn get(self: *const Self, key: K) ?V {
            var idx: u32 = 0;
            while (idx < self.len) : (idx += 1) {
                if (self.keys[idx] == key) {
                    return self.values[idx];
                }
            }

            return null;
        }

        pub fn remove(self: *Self, key: K) ?V {
            var idx: u32 = 0;
            while (idx < self.len) : (idx += 1) {
                if (self.keys[idx] == key) {
                    const val = self.values[idx];
                    self.keys[idx] = self.keys[self.len - 1];
                    self.values[idx] = self.values[self.len - 1];
                    self.len -= 1;
                    return val;
                }
            }

            return null;
        }

        pub fn clear(self: *Self) void {
            self.len = 0;
        }
    };
}
