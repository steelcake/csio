const std = @import("std");
const Alignment = std.mem.Alignment;
const Allocator = std.mem.Allocator;

pub const IoAlloc = struct {
    pub const ALIGN = 512;

    free_sizes: []u32,
    free_ptrs: []usize,
    num_free: u32,
    buf: []align(ALIGN) u8,

    pub fn init(buf: []align(ALIGN) u8, allocator: Allocator) error{OutOfMemory}!IoAlloc {
        std.debug.assert(buf.len % ALIGN == 0);
        const num_slots = buf.len / ALIGN;

        if (buf.len == 0 or num_slots == 0) {
            return .{
                .free_sizes = &.{},
                .free_ptrs = &.{},
                .num_free = 0,
                .buf = &.{},
            };
        }

        const free_sizes = try allocator.alloc(u32, num_slots);
        errdefer allocator.free(free_sizes);
        const free_ptrs = try allocator.alloc(usize, num_slots);
        errdefer allocator.free(free_ptrs);

        free_sizes[0] = @intCast(buf.len);
        free_ptrs[0] = @intFromPtr(buf.ptr);

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
    }

    pub fn alloc(self: *IoAlloc, len: u32) error{OutOfIOMemory}![]align(ALIGN) u8 {
        if (len == 0) {
            return &.{};
        }

        std.debug.assert(len % ALIGN == 0);

        // find first sufficient slot
        var idx: u32 = 0;
        var min_idx: u32 = while (idx < self.num_free) : (idx += 1) {
            const size = self.free_sizes.ptr[idx];
            if (size >= len) {
                break idx;
            }
        } else {
            return error.OutOfIOMemory;
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

        const out = @as([*]align(ALIGN) u8, @ptrFromInt(self.free_ptrs.ptr[min_idx]))[0..len];

        if (min_size == len) {
            // swap-remove the slot from free-list
            self.num_free -= 1;
            self.free_sizes.ptr[min_idx] = self.free_sizes.ptr[self.num_free];
            self.free_ptrs.ptr[min_idx] = self.free_ptrs.ptr[self.num_free];
        } else {
            // shrink the slot in place
            self.free_sizes.ptr[min_idx] = min_size - len;
            self.free_ptrs.ptr[min_idx] = @intFromPtr(out.ptr[len..]);
        }

        @memset(out, 0);

        return out;
    }

    pub fn free(self: *IoAlloc, slice: []align(ALIGN) u8) void {
        if (slice.len == 0) {
            return;
        }

        std.debug.assert(slice.len % ALIGN == 0);

        var addr = @intFromPtr(slice.ptr);
        var size = slice.len;

        // Merge the adjacent free slots together with the new slot.
        // There can be one left and one right free slot adjacent to the one we are freeing now.
        var idx: u32 = 0;
        while (idx < self.num_free) {
            const slot_addr = self.free_ptrs.ptr[idx];
            const slot_size = self.free_sizes.ptr[idx];

            if (slot_addr == addr + size) {
                size +%= slot_size;
                self.num_free -%= 1;
                self.free_ptrs.ptr[idx] = self.free_ptrs.ptr[self.num_free];
                self.free_sizes.ptr[idx] = self.free_sizes.ptr[self.num_free];
            } else if (slot_addr + slot_size == addr) {
                addr = slot_addr;
                self.num_free -%= 1;
                self.free_ptrs.ptr[idx] = self.free_ptrs.ptr[self.num_free];
                self.free_sizes.ptr[idx] = self.free_sizes.ptr[self.num_free];
            } else {
                idx +%= 1;
            }
        }

        // insert new slot
        std.debug.assert(self.num_free < self.free_ptrs.len);
        std.debug.assert(addr >= @intFromPtr(self.buf.ptr));
        std.debug.assert(addr + size <= @intFromPtr(self.buf.ptr) + self.buf.len);
        self.free_ptrs.ptr[self.num_free] = addr;
        self.free_sizes.ptr[self.num_free] = @intCast(size);
        self.num_free += 1;
    }
};
