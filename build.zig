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

    const fuzz = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/fuzz.zig"),
            .target = target,
            .optimize = optimize,
        }),
        // Required for running fuzz tests
        // https://github.com/ziglang/zig/issues/23423
        .use_llvm = true,
    });

    const run_fuzz = b.addRunArtifact(fuzz);

    const fuzz_step = b.step("fuzz", "run fuzz tests");
    fuzz_step.dependOn(&run_fuzz.step);

    const direct_io = b.addExecutable(.{
        .name = "direct_io",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/direct_io.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    direct_io.root_module.addImport("csio", mod);

    const run_direct_io = b.addRunArtifact(direct_io);

    const direct_io_step = b.step("direct_io", "run direct_io example");
    direct_io_step.dependOn(&run_direct_io.step);
}

