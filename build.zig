const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("csio", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });

    const mod_tests = b.addTest(.{
        .root_module = mod,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);

    // Have to have single fuzz entrypoint until this is solved: https://github.com/ziglang/zig/issues/23738
    // So create one target per fuzz target using this function and run the commands seperately.
    add_fuzz_target(b, "fuzz_queue", "src/fuzz_queue.zig", "run fuzz tests for Queue", target, optimize);
    add_fuzz_target(b, "fuzz_slice_map", "src/fuzz_slice_map.zig", "run fuzz tests for SliceMap", target, optimize);
    add_fuzz_target(b, "fuzz_slab", "src/fuzz_slab.zig", "run fuzz tests for Slab", target, optimize);
    add_fuzz_target(b, "fuzz_io_alloc", "src/fuzz_io_alloc.zig", "run fuzz tests for IoAlloc", target, optimize);
}

fn add_fuzz_target(b: *std.Build, command_name: []const u8, root_source_file: []const u8, description: []const u8, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) void {
    const fuzz = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(root_source_file),
            .target = target,
            .optimize = optimize,
        }),
        // Required for running fuzz tests
        // https://github.com/ziglang/zig/issues/23423
        .use_llvm = true,
    });

    const run_fuzz = b.addRunArtifact(fuzz);

    const fuzz_step = b.step(command_name, description);
    fuzz_step.dependOn(&run_fuzz.step);
}
