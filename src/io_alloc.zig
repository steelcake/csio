const std = @import("std");
const Alignment = std.mem.Alignment;
const Allocator = std.mem.Allocator;

const FreeNode = packed struct(u64) {
    next_free: u32,
    size: u32,
};

pub const IoAlloc = struct {
    pub const ALIGN = 512;

    buf_addr: usize,
    buf_len: u32,
    first_free: u32,

    pub fn init(buf: []align(ALIGN) u8) IoAlloc {
        const buf_len: u32 = @intCast(buf.len);
        const buf_addr: usize = @intFromPtr(buf.ptr);

        std.debug.assert(buf_len % ALIGN == 0);

        const free_ptr: *FreeNode = @ptrFromInt(buf_addr);
        free_ptr.size = buf_len;
        free_ptr.next_free = buf_len;

        return .{
            .buf_len = buf_len,
            .buf_addr = buf_addr,
            .first_free = 0,
        };
    }

    pub fn alloc(self: *IoAlloc, len: u32) error{OutOfIOMemory}![]align(ALIGN) u8 {
        std.debug.assert(len % ALIGN == 0);

        if (self.buf_len == self.first_free) {
            return error.OutOfIOMemory;
        }

        if (len == 0) {
            return &.{};
        }

        // find first sufficient slot
        var free_node: *FreeNode = @ptrFromInt(self.buf_addr + self.first_free);
        while (true) {
            const next_free: *FreeNode = @ptrFromInt(self.buf_addr + free_node.next_free);
            if (free_node.size >= len) {} else if (free_node.next_free == self.buf_len) {
                return error.OutOfIOMemory;
            } else {
                free_node = @ptrFromInt(self.buf_addr + free_node.next_free);
            }
        }

        while (free_node.next_free < self.buf_len) {
            if (free_node.size >= len) {
                break;
            } else {
                free_node = @ptrFromInt(self.buf_addr + free_node.next_free);
            }
        } else {
            return error.OutOfIOMemory;
        }

        // find the smallest slot that is sufficient
        var min_node: *FreeNode = free_node;
        while (free_node.next_free < self.buf_len) {
            free_node = @ptrFromInt(self.buf_addr + free_node.next_free);

            if (free_node.size >= len and free_node.size < min_node.size) {
                min_node = free_node;
            }
        }

        const out = @as([*]align(ALIGN) u8, @ptrCast(free_node))[0..len];

        if (free_node.size == len) {
            const next_free_node: *FreeNode = @ptrCast(self.buf_addr + free_node.next_free);
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

        self.active_allocs += 1;

        return out;
    }

    pub fn free(self: *IoAlloc, slice: []align(ALIGN) u8) void {
        if (slice.len == 0) {
            self.active_allocs -= 1;
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

        self.active_allocs -= 1;
    }
};
