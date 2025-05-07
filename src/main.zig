const std = @import("std");
const Allocator = std.mem.Allocator;
const yazap = @import("yazap");
const log = std.log;
const App = yazap.App;
const Arg = yazap.Arg;
const cmd_node_run = @import("node.zig").run;

pub fn main() anyerror!void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var app = App.init(allocator, "scli", null);
    defer app.deinit();

    var scli = app.rootCommand();
    scli.setProperty(.help_on_empty_args);

    var cmd_node = app.createCommand("node", "Node commands");
    cmd_node.setProperty(.help_on_empty_args);

    var cmd_node_stats = app.createCommand("stats", "Node stats");
    const cmd_node_info = app.createCommand("list", "Node list");

    try cmd_node_stats.addArgs(&[_]Arg{
        Arg.booleanOption("cpu", 'c', "Only show CPU stats"),
        Arg.booleanOption("mem", 'm', "Only show Memory stats"),
        Arg.booleanOption("gres", 'g', "Only show GRES stats"),
        Arg.booleanOption("free", 'f', "Only show how much resources are free."),
        Arg.booleanOption("alloc", 'a', "Only show how much resources are allocated."),
        Arg.booleanOption("summary", 's', "Only show total Cluster utilization summary."),
    });

    try cmd_node.addSubcommand(cmd_node_stats);
    try cmd_node.addSubcommand(cmd_node_info);
    try scli.addSubcommand(cmd_node);

    const matches = try app.parseProcess();

    if (matches.subcommandMatches("node")) |node_cmd_args| {
        try cmd_node_run(allocator, node_cmd_args);
    }
}
