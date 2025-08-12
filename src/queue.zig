const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn Queue(comptime T: type) type {
    return struct {
        const Self = @This();

        slots: []T,
        start: u32,
        len: u32,

        pub fn init(capacity: u32, alloc: Allocator) error{OutOfMemory}!Self {
            const slots = try alloc.alloc(T, capacity);

            return .{
                .slots = slots,
                .start = 0,
                .len = 0,
            };
        }

        pub fn deinit(self: Self, alloc: Allocator) void {
            alloc.free(self.slots);
        }

        pub fn push(self: *Self, elem: T) error{OutOfCapacity}!void {
            if (self.len == self.slots.len) {
                return error.OutOfCapacity;
            }

            const end = (self.start + self.len) % self.slots.len;
            self.slots[end] = elem;
            self.len += 1;
        }

        pub fn pop(self: *Self) ?T {
            if (self.len == 0) {
                return null;
            }

            const elem = self.slots[self.start];
            self.start = (self.start + 1) % self.slots.len;
            self.len -= 1;

            return elem;
        }
    };
}
