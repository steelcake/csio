const std = @import("std");
const Alignment = std.mem.Alignment;
const Allocator = std.mem.Allocator;

const ALIGN_B = 512;
const ALIGN = Alignment.fromByteUnits(ALIGN_B);

pub const IoAlloc = struct {
    free_sizes: []u32,
    free_ptrs: [][*]align(ALIGN) u8,
    num_free: u16,
    buf: []align(ALIGN) u8,

    pub fn init(capacity: u32, num_slots: u16, allocator: Allocator) error{OutOfMemory}!IoAlloc {
        if (capacity == 0 or num_slots == 0) {
            return .{
                .free_sizes = &.{},
                .free_ptrs = &.{},
                .num_free = 0,
                .buf = &.{},
            };
        }

        const cap = (capacity + ALIGN_B - 1) / ALIGN_B * ALIGN_B;

        const buf = try allocator.alignedAlloc(u8, ALIGN_B, cap);
        errdefer allocator.free(buf);
        const free_sizes = try allocator.alloc(u32, num_slots);
        errdefer allocator.free(free_sizes);
        const free_ptrs = try allocator.alloc([*]align(ALIGN) u8, num_slots);
        errdefer allocator.free(free_ptrs);

        free_sizes[0] = cap;
        free_ptrs[0] = buf.ptr;

        return .{
            .free_sizes = free_sizes,
            .free_ptrs = free_ptrs,
            .num_free = 1,
            .buf = buf,
        };
    }

    pub fn deinit(self: IoAlloc, allocator: Allocator) void {
        allocator.free(self.free_sizes);
        allocator.free(self.free_ptrs);
        allocator.free(self.buf);
    }

    pub fn alloc(self: *IoAlloc, len: u32) error{OutOfMemory}!IoBuf {
        std.debug.assert(len % ALIGN_B == 0);

        // find first sufficient slot
        var idx: u16 = 0;
        var min_idx: u16 = while (idx < self.num_free) : (idx += 1) {
            const size = self.free_sizes.ptr[idx];
            if (size >= len) {
                break idx;
            }
        } else {
            return error.OutOfMemory;
        };

        // find the smallest slot that is sufficient
        var min_size = self.free_sizes.ptr[min_idx];
        while (idx < self.num_free) : (idx += 1) {
            const size = self.free_sizes.ptr[idx];
            if (size >= len and size < min_size) {
                min_idx = idx;
                min_size = size;
            }
        }

        const out = self.free_ptrs.ptr[min_idx][0..len];

        if (min_size == len) {
            // swap-remove the slot from free-list
            self.num_free -= 1;
            self.free_sizes.ptr[min_idx] = self.free_sizes.ptr[self.num_free];
            self.free_ptrs.ptr[min_idx] = self.free_ptrs.ptr[self.num_free];
        } else {
            // shrink the slot in place
            self.free_sizes.ptr[min_idx] = min_size - len;
            self.free_ptrs.ptr[min_idx] = out.ptr[len..];
        }

        return .{ ._alloc = self, ._alloc_buf = out, .buf = out };
    }

    pub fn free(self: *IoAlloc, slice: []align(ALIGN) u8) void {
        std.debug.assert(slice.len % ALIGN_B == 0);

        const start = @intFromPtr(slice.ptr);
        const end = start + slice.len;

        var idx: u16 = 0;
        while (idx < self.num_free) : (idx += 1) {
            const ptr = @intFromPtr(self.free_ptrs.ptr[idx]);
            const size = self.free_sizes.ptr[idx];

            if (ptr == end) {
                // merge from left
                self.free_ptrs.ptr[idx] = @ptrFromInt(start);
                self.free_sizes.ptr[idx] = size + @as(u32, @intCast(slice.len));
                break;
            } else if (ptr + size == end) {
                // merge from right
                self.free_sizes.ptr[idx] = size + @as(u32, @intCast(slice.len));
                break;
            }
        } else {
            // insert new slot if couldn't find an adjacent free slot
            std.debug.assert(self.num_free < self.free_ptrs.len);
            std.debug.assert(@intFromPtr(slice.ptr) >= @intFromPtr(self.buf.ptr));
            std.debug.assert(@intFromPtr(slice.ptr) + slice.len <= @intFromPtr(self.buf.ptr) + self.buf.len);

            self.free_ptrs.ptr[self.num_free] = slice.ptr;
            self.free_sizes.ptr[self.num_free] = @intCast(slice.len);
            self.num_free += 1;
        }
    }
};
