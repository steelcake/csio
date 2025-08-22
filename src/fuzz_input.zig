const std = @import("std");
const Allocator = std.mem.Allocator;
const Prng = std.Random.DefaultPrng;

const Error = error{ OutOfMemory, ShortInput };

/// This struct implements structured fuzzing.
///
/// It generates data based on given fuzzer generated random data.
/// Normal random number generator seeded by fuzzer input can be used in places where the specific values don't change execution but we want them to be random.
pub const FuzzInput = struct {
    data: []const u8,

    pub fn init(data: []const u8) FuzzInput {
        return .{
            .data = data,
        };
    }

    pub fn float(self: *FuzzInput, comptime T: type) Error!T {
        var prng = try self.make_prng();
        const rand = prng.random();
        return @floatCast(rand.float(f64));
    }

    pub fn int(self: *FuzzInput, comptime T: type) Error!T {
        const size = @sizeOf(T);
        if (self.data.len < size) {
            return Error.ShortInput;
        }
        const i = std.mem.readVarInt(T, self.data[0..size], .little);
        self.data = self.data[size..];
        return i;
    }

    pub fn boolean(self: *FuzzInput) Error!bool {
        if (self.data.len == 0) {
            return Error.ShortInput;
        }
        const byte = self.data[0];
        self.data = self.data[1..];
        return byte % 2 == 0;
    }

    pub fn make_prng(self: *FuzzInput) Error!Prng {
        const seed = try self.int(u64);
        return Prng.init(seed);
    }

    pub fn bytes(self: *FuzzInput, len: u32) Error![]const u8 {
        if (self.data.len < len) {
            return Error.ShortInput;
        }
        const b = self.data[0..len];
        self.data = self.data[len..];
        return b;
    }

    pub fn slice(self: *FuzzInput, comptime T: type, len: u32, alloc: Allocator) Error![]T {
        const out = try alloc.alloc(T, len);
        const out_raw: []u8 = @ptrCast(out);
        @memcpy(out_raw, try self.bytes(@intCast(out_raw.len)));
        return out;
    }
};
