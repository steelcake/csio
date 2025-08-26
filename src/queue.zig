const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn Queue(comptime T: type) type {
    return struct {
        const Self = @This();

        slots: []T,
        start: u32,
        capacity: u32,
        capacity_mask: u32,
        len: u32,

        pub fn init(capacity: u32, alloc: Allocator) error{OutOfMemory}!Self {
            if (capacity == 0) {
                return .{
                    .slots = &.{},
                    .start = 0,
                    .len = 0,
                    .capacity = 0,
                    .capacity_mask = 0,
                };
            }

            const cap = std.math.ceilPowerOfTwo(u32, capacity) catch unreachable;
            const mask = cap - 1;

            const slots = try alloc.alloc(T, cap);

            return .{
                .slots = slots,
                .start = 0,
                .len = 0,
                .capacity = cap,
                .capacity_mask = mask,
            };
        }

        pub fn deinit(self: Self, alloc: Allocator) void {
            alloc.free(self.slots);
        }

        pub fn length(self: *const Self) u32 {
            return self.len;
        }

        pub fn is_empty(self: *const Self) bool {
            return self.len == 0;
        }

        pub fn clear(self: *Self) void {
            self.start = 0;
            self.len = 0;
        }

        pub fn push(self: *Self, elem: T) error{OutOfCapacity}!void {
            if (self.len == self.capacity) {
                return error.OutOfCapacity;
            }

            const end = (self.start +% self.len) & self.capacity_mask;
            self.slots.ptr[end] = elem;
            self.len +%= 1;
        }

        pub fn pop(self: *Self) ?T {
            if (self.len == 0) {
                return null;
            }

            const elem = self.slots.ptr[self.start];
            self.start = (self.start +% 1) & self.capacity_mask;
            self.len -%= 1;

            return elem;
        }
    };
}
