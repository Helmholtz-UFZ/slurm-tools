const std = @import("std");
const slurm = @import("slurm");
const pt = @import("prettytable");
const clap = @import("clap");
const allocPrint = std.fmt.allocPrint;
const Allocator = std.mem.Allocator;

pub const GresUtilCollection = std.ArrayList(GresUtil);

pub fn fmtGresUtilCollection(gres: GresUtilCollection, allocator: Allocator) ![]const u8 {
    var tres_fmt = std.ArrayList(u8).init(allocator);
    defer tres_fmt.deinit();

    for (gres.items) |t| {
        const gres_str = try t.fmtIdleAlloc(allocator);
        try tres_fmt.appendSlice(gres_str);
    }

    _ = tres_fmt.pop();
    return tres_fmt.toOwnedSlice();
}

pub fn getGresUtil(gres: []const u8, allocator: Allocator) !GresCollection {
    //const alloc_tres = node.allocTres();
    const alloc_tres = slurm.TresString.init(gres);
    // maybe use defer if (alloc_tres) |t| { ... }
    defer alloc_tres.deinit();
    var alloc_tres_iter = alloc_tres.iter();
    var collection = GresCollection.init(allocator);

    while (alloc_tres_iter.next()) |kv| {
        if (!std.mem.startsWith(u8, kv, "gres")) continue;
        const tres = try Gres.fromKVPair(kv);

        try collection.append(tres);
    }

    return collection;
}

pub const GresUtil = struct {
    name: []const u8,
    type: ?[]const u8 = null,
    name_and_type: []const u8,
    alloc: u64 = 0,
    idle: u64 = 0,
    total: u64 = 0,

    pub fn fromParts(alloc_gres: GresCollection, cfg_gres: GresCollection, allocator: Allocator) !GresUtilCollection {
        var gres_util = GresUtilCollection.init(allocator);

        for (cfg_gres.items) |cg| {
            var util = GresUtil{
                .name = cg.name,
                .type = cg.type,
                .name_and_type = cg.name_and_type,
                .alloc = 0,
                .idle = cg.count,
                .total = cg.count,
            };

            for (alloc_gres.items) |ag| {
                if (std.mem.eql(u8, ag.name_and_type, cg.name_and_type)) {
                    util.alloc = ag.count;
                    util.idle = cg.count - ag.count;
                    util.total = cg.count;
                }
            }
            try gres_util.append(util);
        }
        return gres_util;
    }

    pub fn fmtIdleAlloc(self: GresUtil, allocator: Allocator) ![]const u8 {
        if (self.alloc == 0) return "";

        const name = self.name[std.mem.indexOfScalar(u8, self.name, '/').? + 1 ..];
        var s: []const u8 = undefined;
        if (self.type) |typ| {
            s = try allocPrint(allocator, "   {s}={d}/{d} ({d} idle)\n", .{ typ, self.alloc, self.total, self.idle });
        } else {
            s = try allocPrint(allocator, "{s}={d}/{d} ({d} idle)\n", .{ name, self.alloc, self.total, self.idle });
        }

        return s;
    }

    pub fn fmtAlloc(self: GresUtil, allocator: Allocator) ![]const u8 {
        if (self.alloc == 0) return "";

        const name = self.name[std.mem.indexOfScalar(u8, self.name, '/').? + 1 ..];
        var s: []const u8 = undefined;
        if (self.type) |typ| {
            s = try allocPrint(allocator, "   {s}={d}\n", .{ typ, self.alloc });
        } else {
            s = try allocPrint(allocator, "{s}={d}\n", .{ name, self.alloc });
        }

        return s;
    }

    pub fn fmtIdle(self: GresUtil, allocator: Allocator) ![]const u8 {
        if (self.idle == 0) return "";

        const name = self.name[std.mem.indexOfScalar(u8, self.name, '/').? + 1 ..];
        var s: []const u8 = undefined;
        if (self.type) |typ| {
            s = try allocPrint(allocator, "   {s}={d}\n", .{ typ, self.idle });
        } else {
            s = try allocPrint(allocator, "{s}={d}\n", .{ name, self.idle });
        }

        return s;
    }
};

pub const Gres = struct {
    name: []const u8,
    type: ?[]const u8,
    name_and_type: []const u8,
    count: u64,

    pub fn fromKVPair(kv: []const u8) !Gres {
        var it = std.mem.splitScalar(u8, kv, '=');
        const name_and_type = it.first();
        const count = try std.fmt.parseInt(u64, it.rest(), 10);

        var it2 = std.mem.splitScalar(u8, name_and_type, ':');
        const name = it2.first();
        var typ: ?[]const u8 = null;
        if (it2.next()) |t| {
            typ = t;
        }

        return .{
            .name = name,
            .type = typ,
            .name_and_type = name_and_type,
            .count = count,
        };
    }

    pub fn fmtGres(self: Gres, allocator: Allocator) ![]const u8 {
        const name = self.name[std.mem.indexOfScalar(u8, self.name, '/').? + 1 ..];

        var s: []const u8 = undefined;
        if (self.type) |typ| {
            s = try allocPrint(allocator, "   {s}={d}\n", .{ typ, self.count });
        } else {
            s = try allocPrint(allocator, "{s}={d}\n", .{ name, self.count });
        }

        return s;
    }
};

pub const GresCollection = std.ArrayList(Gres);

fn humanize(allocator: Allocator, val: u64) ![]const u8 {
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

fn show_node_total_util(allocator: Allocator, util: slurm.Node.Utilization, gres: std.ArrayList(GresCollection)) !void {
    var tstats = pt.Table.init(allocator);
    defer tstats.deinit();

    tstats.setFormat(pt.FORMAT_NO_BORDER);
    try tstats.setTitle(&.{ "Resource", "Total", "Alloc", "Idle" });

    const alloc_cpus_percent = to_percent(f64, util.alloc_cpus, util.total_cpus);
    const idle_cpus_percent = to_percent(f64, util.idle_cpus, util.total_cpus);

    try tstats.addRow(&.{
        "CPU",
        try allocPrint(allocator, "{d}", .{util.total_cpus}),
        try allocPrint(allocator, "{d} ({d:.2}%)", .{ util.alloc_cpus, alloc_cpus_percent }),
        try allocPrint(allocator, "{d} ({d:.2}%)", .{ util.idle_cpus, idle_cpus_percent }),
    });

    const alloc_mem_percent = to_percent(f64, util.alloc_memory, util.real_memory);
    const idle_mem_percent = to_percent(f64, util.idle_memory, util.real_memory);
    const total_mem_hum = try humanize(allocator, util.real_memory);
    const alloc_mem_hum = try humanize(allocator, util.alloc_memory);
    const idle_mem_hum = try humanize(allocator, util.idle_memory);

    try tstats.addRow(&.{
        "Memory",
        total_mem_hum,
        try allocPrint(allocator, "{s} ({d:.2}%)", .{ alloc_mem_hum, alloc_mem_percent }),
        try allocPrint(allocator, "{s} ({d:.2}%)", .{ idle_mem_hum, idle_mem_percent }),
    });

    _ = gres;

    try tstats.printstd();
}

pub fn fmtGres(gres: GresCollection, allocator: Allocator) ![]const u8 {
    var tres_fmt = std.ArrayList(u8).init(allocator);
    defer tres_fmt.deinit();

    for (gres.items) |t| {
        const tres_str = try t.fmtGres(allocator);
        try tres_fmt.appendSlice(tres_str);
    }

    _ = tres_fmt.pop();
    return tres_fmt.toOwnedSlice();
}

pub fn show_nodes(allocator: Allocator, res: anytype, stdout: anytype) !void {
    var table = pt.Table.init(allocator);
    defer table.deinit();
    const arg_free = res.args.free != 0;
    var arg_cpu = res.args.cpu != 0;
    var arg_mem = res.args.mem != 0;
    var arg_gres = res.args.gres != 0;

    const specific_resource = arg_cpu or arg_mem or arg_gres;
    if (!specific_resource) {
        arg_cpu = true;
        arg_mem = true;
        arg_gres = true;
    }

    const arg_alloc = res.args.alloc != 0;

    var title = std.ArrayList([]const u8).init(allocator);
    try title.appendSlice(&.{"Nodename"});

    if (arg_free) {
        if (arg_cpu) try title.appendSlice(&.{"IdleCPUs"});
        if (arg_mem) try title.appendSlice(&.{"IdleMemory"});
        if (arg_gres) try title.appendSlice(&.{"IdleGRES"});
    } else if (arg_alloc) {
        if (arg_cpu) try title.appendSlice(&.{"AllocCPUs"});
        if (arg_mem) try title.appendSlice(&.{"AllocMemory"});
        if (arg_gres) try title.appendSlice(&.{"AllocGRES"});
    } else {
        if (arg_cpu) try title.appendSlice(&.{"CPUs (A/I/T)"});
        if (arg_mem) try title.appendSlice(&.{"Memory (A/I/T)"});
        if (arg_gres) try title.appendSlice(&.{"GRES (A/I/T)"});
    }
    try table.setTitle(try title.toOwnedSlice());

    var node_resp = try slurm.Node.loadAll();
    defer node_resp.deinit();

    var total_util = slurm.Node.Utilization{};
    var total_util_gres = std.ArrayList(GresCollection).init(allocator);

    var node_iter = node_resp.iter();
    while (node_iter.next()) |node| {
        // node.name might be null if there are Nodes in the slurm.conf which
        // physically do not exist anymore.
        const node_name = slurm.parseCStr(node.name) orelse continue;

        //      const cfg_tres = node.configuredTres();
        //      std.debug.print("CfgTres {s}\n", .{cfg_tres.str});

        const util = node.utilization();
        const idle_cpus = try allocPrint(allocator, "{d}", .{util.idle_cpus});
        const alloc_cpus = try allocPrint(allocator, "{d}", .{util.alloc_cpus});
        const alloc_memory = try humanize(allocator, util.alloc_memory);
        const idle_memory = try humanize(allocator, util.idle_memory);

        const alloc_gres_str = "gres/gpu=3,gres/gpu:nvidia-a100=3";
        const alloc_gres = try getGresUtil(alloc_gres_str, allocator);
        //        const alloc_gres_fmt = try fmtGres(alloc_gres, allocator);

        const cfg_gres_str = "gres/gpu=20,gres/gpu:nvidia-a100=10,gres/gpu:nvidia-h100=10";
        const cfg_gres = try getGresUtil(cfg_gres_str, allocator);

        const gres_util = try GresUtil.fromParts(alloc_gres, cfg_gres, allocator);

        for (gres_util.items) |gu| {
            std.debug.print("name_and_type: {s} | alloc: {d} | idle: {d} | total: {d}\n", .{ gu.name_and_type, gu.alloc, gu.idle, gu.total });
        }

        var data = std.ArrayList([]const u8).init(allocator);
        try data.appendSlice(&.{node_name});

        if (arg_free) {
            if (arg_cpu) try data.appendSlice(&.{idle_cpus});
            if (arg_mem) try data.appendSlice(&.{idle_memory});
            if (arg_gres) try data.appendSlice(&.{"0"});
        } else if (arg_alloc) {
            if (arg_cpu) try data.appendSlice(&.{alloc_cpus});
            if (arg_mem) try data.appendSlice(&.{alloc_memory});
            if (arg_gres) try data.appendSlice(&.{"0"});
        } else {
            if (arg_cpu) {
                const fmt = try allocPrint(allocator, "{d}/{d}/{d}", .{ util.alloc_cpus, util.idle_cpus, util.total_cpus });
                try data.appendSlice(&.{fmt});
            }

            if (arg_mem) {
                const total_memory = try humanize(allocator, util.real_memory);
                const fmt = try allocPrint(allocator, "{s}/{s}/{s}", .{ alloc_memory, idle_memory, total_memory });
                try data.appendSlice(&.{fmt});
            }

            if (arg_gres) {
                try data.appendSlice(&.{try fmtGresUtilCollection(gres_util, allocator)});
            }
        }
        try table.addRow(try data.toOwnedSlice());

        total_util.add(util);
        try total_util_gres.append(alloc_gres);
    }

    if (res.args.stats == 0) {
        try table.printstd();
        try stdout.print("\n", .{});
    }

    try show_node_total_util(allocator, total_util, total_util_gres);
}

pub fn main() !void {
    slurm.init(null);
    defer slurm.deinit();

    const stdout = std.io.getStdOut().writer();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const params = comptime clap.parseParamsComptime(
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

    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, clap.parsers.default, .{
        .diagnostic = &diag,
        .allocator = allocator,
    }) catch |err| {
        // Report useful error and exit
        diag.report(std.io.getStdErr().writer(), err) catch {};
        return err;
    };
    defer res.deinit();

    if (res.args.help != 0)
        return clap.help(
            std.io.getStdErr().writer(),
            clap.Help,
            &params,
            .{ .spacing_between_parameters = 0 },
        );

    const part_arg = res.args.part == 0;
    const node_arg = part_arg or res.args.nodes != 0;

    if (node_arg) try show_nodes(allocator, res, stdout);
    if (part_arg) show_parts();
}
