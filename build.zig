const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const snodestats_dep = b.dependency("snodestats", .{
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(snodestats_dep.artifact("snodestats"));
}
