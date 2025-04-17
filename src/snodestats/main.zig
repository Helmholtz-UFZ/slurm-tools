const std = @import("std");
const slurm = @import("slurm");
const Gres = slurm.gres.Gres;
const pt = @import("prettytable");
const clap = @import("clap");
const allocPrint = std.fmt.allocPrint;
const Allocator = std.mem.Allocator;

const params = clap.parseParamsComptime(
    \\-h, --help             Display this help and exit.
    \\-f, --free             Only show how much resources are free.
    \\-c, --cpu              Only show CPU statistics.
    \\-m, --mem              Only show Memory statistics.
    \\-g, --gres             Only show GRES (GPU) statistics.
    \\-a, --alloc            Only show how much resources are allocated.
    \\-n, --nodes            Show node-related utlization. This is the default.
    \\-s, --stats            Only show total Cluster utilization summary.
    \\-p, --part             Show partition related utization.
);

pub const GresUtil = struct {
    name: []const u8,
    type: ?[]const u8 = null,
    name_and_type: []const u8,
    alloc: u64 = 0,
    idle: u64 = 0,
    total: u64 = 0,

    pub const FormatType = enum {
        idle,
        alloc,
        idleAndAlloc,
    };

    pub const Collection = std.ArrayList(GresUtil);

    pub fn fmtCollection(gres: GresUtil.Collection, allocator: Allocator, fmt_type: GresUtil.FormatType) ![]const u8 {
        var tres_fmt = std.ArrayList(u8).init(allocator);
        defer tres_fmt.deinit();

        for (gres.items) |t| {
            const gres_str = try t.fmt(allocator, fmt_type);
            try tres_fmt.appendSlice(gres_str);
        }

        _ = tres_fmt.pop();
        return tres_fmt.toOwnedSlice();
    }

    pub fn combine(alloc_gres: ?[:0]const u8, cfg_gres: [:0]const u8, allocator: Allocator) !?GresUtil.Collection {
        var gres_util = GresUtil.Collection.init(allocator);
        var cfg_gres_iter = Gres.iter(cfg_gres, ',');

        while (cfg_gres_iter.next()) |cg| {
            var util = GresUtil{
                .name = cg.name,
                .type = cg.type,
                .name_and_type = cg.name_and_type,
                .alloc = 0,
                .idle = cg.count,
                .total = cg.count,
            };

            if (alloc_gres) |gres| {
                var alloc_gres_iter = Gres.iter(gres, ',');
                while (alloc_gres_iter.next()) |ag| {
                    if (std.mem.eql(u8, ag.name_and_type, cg.name_and_type)) {
                        util.alloc = ag.count;
                        util.idle = cg.count - ag.count;
                        util.total = cg.count;
                    }
                }
            }

            try gres_util.append(util);
        }

        if (gres_util.items.len > 0) {
            return gres_util;
        } else {
            gres_util.deinit();
            return null;
        }
    }

    pub fn fmt(self: GresUtil, allocator: Allocator, fmt_type: FormatType) ![]const u8 {
        const name_or_type = if (self.type) |typ|
            try allocPrint(allocator, "   {s}", .{typ})
        else
            self.name;

        return switch (fmt_type) {
            .idle => {
                if (self.idle == 0) return "";
                return try allocPrint(allocator, "{s}={d}\n", .{
                    name_or_type,
                    self.idle,
                });
            },
            .alloc => {
                if (self.alloc == 0) return "";
                return try allocPrint(allocator, "{s}={d}\n", .{
                    name_or_type,
                    self.alloc,
                });
            },
            .idleAndAlloc => {
                if (self.alloc == 0 and self.idle == 0) return "";
                return try allocPrint(allocator, "{s}={d}/{d} ({d} idle)\n", .{
                    name_or_type,
                    self.alloc,
                    self.total,
                    self.idle,
                });
            },
        };
    }
};

fn humanize(allocator: Allocator, val: u128) ![]const u8 {
    const units = [_][]const u8{ "M", "G", "T", "P", "E", "Z" };
    var fl: f64 = @floatFromInt(val);

    for (units) |unit| {
        if (@abs(fl) < 1024.0) {
            return try allocPrint(
                allocator,
                "{d:.2}{s}",
                .{ fl, unit },
            );
        }
        fl /= 1024.0;
    }

    return try allocPrint(
        allocator,
        "{d:.2}{s}",
        .{ fl, "Y" },
    );
}

fn to_percent(comptime T: type, numerator: anytype, denominator: anytype) T {
    const n: T = @floatFromInt(numerator);
    const d: T = @floatFromInt(denominator);
    return (n / d) * 100.0;
}

pub fn show_parts() void {}

fn fmtTotalMemoryStats(table: *pt.Table, allocator: Allocator, util: slurm.Node.Utilization) !void {
    const alloc_percent = to_percent(f64, util.alloc_memory, util.real_memory);
    const idle_percent = to_percent(f64, util.idle_memory, util.real_memory);
    const total_mem = try humanize(allocator, util.real_memory);
    const alloc_mem = try humanize(allocator, util.alloc_memory);
    const idle_mem = try humanize(allocator, util.idle_memory);

    try table.addRow(&.{
        "Memory",
        total_mem,
        try allocPrint(allocator, "{s} ({d:.2}%)", .{ alloc_mem, alloc_percent }),
        try allocPrint(allocator, "{s} ({d:.2}%)", .{ idle_mem, idle_percent }),
    });
}

fn fmtTotalCPUStats(table: *pt.Table, allocator: Allocator, util: slurm.Node.Utilization) !void {
    const alloc_percent = to_percent(f64, util.alloc_cpus, util.total_cpus);
    const idle_percent = to_percent(f64, util.idle_cpus, util.total_cpus);

    try table.addRow(&.{
        "CPU",
        try allocPrint(allocator, "{d}", .{util.total_cpus}),
        try allocPrint(allocator, "{d} ({d:.2}%)", .{ util.alloc_cpus, alloc_percent }),
        try allocPrint(allocator, "{d} ({d:.2}%)", .{ util.idle_cpus, idle_percent }),
    });
}

fn fmtTotalGresStats(table: *pt.Table, allocator: Allocator, gres: std.StringArrayHashMap(GresUtil)) !void {
    var header: std.ArrayList(u8) = .init(allocator);
    var total_gres: std.ArrayList(u8) = .init(allocator);
    var alloc_gres: std.ArrayList(u8) = .init(allocator);
    var idle_gres: std.ArrayList(u8) = .init(allocator);

    var g_iter = gres.iterator();

    if (g_iter.len == 0) return;

    while (g_iter.next()) |kv| {
        const total = kv.value_ptr.total;
        const alloc = kv.value_ptr.alloc;
        const idle = kv.value_ptr.idle;

        const name = if (kv.value_ptr.type) |typ|
            try allocPrint(allocator, "   {s}", .{typ})
        else
            try allocPrint(allocator, "gres/{s}", .{kv.value_ptr.name_and_type});

        const new_line = if (g_iter.index < g_iter.len) "\n" else "";

        const alloc_gres_percent = to_percent(f64, alloc, total);
        const idle_gres_percent = to_percent(f64, idle, total);

        try header.appendSlice(try allocPrint(allocator, "{s}{s}", .{
            name,
            new_line,
        }));
        try total_gres.appendSlice(try allocPrint(allocator, "{d}{s}", .{
            total,
            new_line,
        }));
        try alloc_gres.appendSlice(try allocPrint(allocator, "{d} ({d:.2}%){s}", .{
            alloc,
            alloc_gres_percent,
            new_line,
        }));
        try idle_gres.appendSlice(try allocPrint(allocator, "{d} ({d:.2}%){s}", .{
            idle,
            idle_gres_percent,
            new_line,
        }));
    }

    try table.addRow(&.{
        try header.toOwnedSlice(),
        try total_gres.toOwnedSlice(),
        try alloc_gres.toOwnedSlice(),
        try idle_gres.toOwnedSlice(),
    });
}

fn show_node_total_util(allocator: Allocator, util: slurm.Node.Utilization, gres: std.StringArrayHashMap(GresUtil)) !void {
    var tstats = pt.Table.init(allocator);
    defer tstats.deinit();

    tstats.setFormat(pt.FORMAT_NO_BORDER);
    try fmtTotalCPUStats(&tstats, allocator, util);
    try fmtTotalMemoryStats(&tstats, allocator, util);
    try fmtTotalGresStats(&tstats, allocator, gres);

    try tstats.setTitle(&.{ "Resource", "Total", "Alloc", "Idle" });
    for (tstats.titles.?.cells.items) |*cell| {
        cell.setAlign(pt.Alignment.center);
    }
    try tstats.printstd();
}

pub fn show_nodes(allocator: Allocator, args: Args, stdout: anytype) !void {
    var table = pt.Table.init(allocator);
    defer table.deinit();

    var title = std.ArrayList([]const u8).init(allocator);
    try title.appendSlice(&.{ "Nodename", "State" });

    if (args.free) {
        if (args.cpu) try title.appendSlice(&.{"IdleCPUs"});
        if (args.mem) try title.appendSlice(&.{"IdleMemory"});
        if (args.gres) try title.appendSlice(&.{"IdleGRES"});
    } else if (args.alloc) {
        if (args.cpu) try title.appendSlice(&.{"AllocCPUs"});
        if (args.mem) try title.appendSlice(&.{"AllocMemory"});
        if (args.gres) try title.appendSlice(&.{"AllocGRES"});
    } else {
        if (args.cpu) try title.appendSlice(&.{"CPUs (A/I/T)"});
        if (args.mem) try title.appendSlice(&.{"Memory (A/I/T)"});
        if (args.gres) try title.appendSlice(&.{"GRES"});
    }

    try table.setTitle(try title.toOwnedSlice());
    for (table.titles.?.cells.items) |*cell| {
        cell.setAlign(pt.Alignment.center);
    }

    table.setFormat(pt.FORMAT_BOX_CHARS);

    var node_resp = try slurm.loadNodes();
    defer node_resp.deinit();

    var total_util = slurm.Node.Utilization{};
    var total_util_gres: std.StringArrayHashMap(GresUtil) = .init(allocator);

    var node_iter = node_resp.iter();
    while (node_iter.next()) |node| {
        // node.name might be null if there are Nodes in the slurm.conf which
        // physically do not exist anymore.
        const node_name = slurm.parseCStr(node.name) orelse continue;

        const state = node.state;
        const invalid_base_states = state.base != .idle and state.base != .mixed;
        const invalid_state_flags = state.flags.reservation or state.flags.drain;

        if (args.free and (invalid_base_states or invalid_state_flags)) continue;

        const util = node.utilization();
        const idle_cpus = try allocPrint(allocator, "{d}", .{util.idle_cpus});
        const alloc_cpus = try allocPrint(allocator, "{d}", .{util.alloc_cpus});
        const alloc_memory = try humanize(allocator, util.alloc_memory);
        const idle_memory = try humanize(allocator, util.idle_memory);

        const alloc_tres = try node.allocTres(allocator);
        // const alloc_gres = "gres/gpu=3,gres/gpu:nvidia-a100=3";

        var gres_util: ?GresUtil.Collection = null;
        if (node.tres_fmt_str != null) {
            // const cfg_gres = "gres/gpu=20,gres/gpu:nvidia-a100=10,gres/gpu:nvidia-h100=10";
            // gres_util = try GresUtil.combine(alloc_gres, cfg_gres, allocator);
            gres_util = try GresUtil.combine(
                alloc_tres,
                slurm.parseCStrZ(node.tres_fmt_str).?,
                allocator,
            );

            if (gres_util) |gutil| {
                for (gutil.items) |gu| {
                    const gg = try total_util_gres.getOrPut(gu.name_and_type);
                    if (gg.found_existing) {
                        gg.value_ptr.alloc += gu.alloc;
                        gg.value_ptr.idle += gu.idle;
                        gg.value_ptr.total += gu.total;
                    } else {
                        gg.value_ptr.* = gu;
                    }
                }
            }
        }

        var data = std.ArrayList([]const u8).init(allocator);
        try data.appendSlice(&.{node_name});
        try data.appendSlice(&.{try state.toStr(allocator)});

        if (args.free) {
            if (args.cpu) try data.appendSlice(&.{idle_cpus});
            if (args.mem) try data.appendSlice(&.{idle_memory});
            if (args.gres and gres_util != null) try data.appendSlice(&.{try GresUtil.fmtCollection(gres_util.?, allocator, .idle)});
        } else if (args.alloc) {
            if (args.cpu) try data.appendSlice(&.{alloc_cpus});
            if (args.mem) try data.appendSlice(&.{alloc_memory});
            if (args.gres and gres_util != null) try data.appendSlice(&.{try GresUtil.fmtCollection(gres_util.?, allocator, .alloc)});
        } else {
            if (args.cpu) {
                const fmt = try allocPrint(allocator, "{d: >2} / {d: >2} / {d: >2}", .{
                    util.alloc_cpus,
                    util.idle_cpus,
                    util.total_cpus,
                });
                try data.appendSlice(&.{fmt});
            }

            if (args.mem) {
                const total_memory = try humanize(allocator, util.real_memory);
                const fmt = try allocPrint(allocator, "{s: >7} / {s : >7} / {s: >7}", .{
                    alloc_memory,
                    idle_memory,
                    total_memory,
                });
                try data.appendSlice(&.{fmt});
            }

            if (args.gres and gres_util != null) {
                try data.appendSlice(&.{try GresUtil.fmtCollection(gres_util.?, allocator, .idleAndAlloc)});
            }
        }

        if (args.gres and !args.cpu and !args.mem and gres_util == null) continue;
        total_util.add(util);
        try table.addRow(try data.toOwnedSlice());
    }

    if (!args.stats) {
        try table.printstd();
        try stdout.print("\n", .{});
    }

    try show_node_total_util(allocator, total_util, total_util_gres);
}

pub const Args = struct {
    partitions: bool,
    nodes: bool,
    free: bool,
    alloc: bool,
    cpu: bool,
    mem: bool,
    gres: bool,
    stats: bool,
    help: bool,

    clap_result: clap.Result(clap.Help, &params, clap.parsers.default),

    pub fn deinit(self: Args) void {
        self.clap_result.deinit();
    }
};

pub fn parseArgs(allocator: Allocator) !Args {
    var diag = clap.Diagnostic{};
    const res = clap.parse(clap.Help, &params, clap.parsers.default, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        // Report useful error and exit
        diag.report(std.io.getStdErr().writer(), err) catch {};
        return err;
    };

    var args: Args = .{
        .partitions = res.args.part != 0,
        .nodes = res.args.part == 0 or res.args.nodes != 0,
        .free = res.args.free != 0,
        .alloc = res.args.alloc != 0,
        .cpu = res.args.cpu != 0,
        .mem = res.args.mem != 0,
        .gres = res.args.gres != 0,
        .stats = res.args.stats != 0,
        .help = res.args.help != 0,
        .clap_result = res,
    };

    if (!args.cpu and !args.mem and !args.gres) {
        args.cpu = true;
        args.mem = true;
        args.gres = true;
    }

    return args;
}

pub fn main() !void {
    slurm.init(null);
    defer slurm.deinit();

    const stdout = std.io.getStdOut().writer();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const args = try parseArgs(allocator);
    defer args.deinit();

    if (args.help) {
        return clap.help(
            std.io.getStdErr().writer(),
            clap.Help,
            &params,
            .{ .spacing_between_parameters = 0 },
        );
    }

    if (args.nodes) try show_nodes(allocator, args, stdout);
    if (args.partitions) show_parts();
}
