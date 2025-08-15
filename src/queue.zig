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

        pub fn length(self: *const Self) u32 {
            return self.len;
        }

        pub fn clear(self: *Self) void {
            self.start = 0;
            self.len = 0;
        }

        pub fn push_batch(self: *Self, elems: []const T) error{OutOfCapacity}!void {
            if (self.len + elems.len > self.slots.len) {
                return error.OutOfCapacity;
            }

            // before wrapping around
            const n = @min(self.slots.len - self.start, elems.len);
            @memcpy(self.slots[self.start .. self.start + n], elems[0..n]);

            // after wrapping around
            if (n < elems.len) {
                const m = elems.len - n;
                @memcpy(self.slots[0..m], elems[n..]);
            }

            self.len += elems.len;
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
