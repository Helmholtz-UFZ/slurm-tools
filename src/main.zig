const std = @import("std");
const Allocator = std.mem.Allocator;
const yazap = @import("yazap");
const log = std.log;
const App = yazap.App;
const Arg = yazap.Arg;
const cmd_node_run = @import("node.zig").run;
const cmd_job_run = @import("job.zig").run;

pub fn main() anyerror!void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const options: App.Options = .{
        .display_help = true,
        .help_options = .{
            .line = .{
                .options = .{
                    .format = .description_separate,
                    .signature_left_padding = 2,
                    .signature_max_width = 30,
                    .description_left_padding = 10,
                },
                .subcommand = .{
                    .format = .description_separate,
                    .description_left_padding = 8,
                },
            },
        },
    };

    var app = App.initWithOptions(
        allocator,
        "scli",
        "A program that allows showing and controlling various aspects of Slurm.",
        options,
    );
    defer app.deinit();

    var scli = app.rootCommand();
    scli.setProperty(.help_on_empty_args);

    var cmd_node = app.createCommand(
        "node",
        "Commands that work with Slurm's Node API",
    );
    cmd_node.setProperty(.help_on_empty_args);

    var cmd_node_stats = app.createCommand("stats", "Node stats");
    const cmd_node_info = app.createCommand("list", "Node list");

    try cmd_node_stats.addArgs(&[_]Arg{
        Arg.booleanOption("cpu", 'c', "Only show CPU stats"),
        Arg.booleanOption("mem", 'm', "Only show Memory stats"),
        Arg.booleanOption("gres", 'g', "Only show GRES stats"),
        Arg.booleanOption("free", 'f', "Only show how much resources are free."),
        Arg.booleanOption("alloc", 'a', "Only show how much resources are allocated."),
        Arg.booleanOption("summary", 's', "Show total Cluster utilization summary."),
        Arg.multiValuesOption("states", 'S',
            \\Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam
            \\nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam
            \\erat, sed diam voluptua. At vero eos et accusam et justo duo
            \\dolores et ea rebum. Stet clita kasd gubergren,
        , 50),
    });

    try cmd_node.addSubcommand(cmd_node_stats);
    try cmd_node.addSubcommand(cmd_node_info);

    var cmd_job = app.createCommand("job", "Job commands");
    cmd_job.setProperty(.help_on_empty_args);

    var cmd_job_stat = app.createCommand("stat", "Show performance statistics for Jobs.");
    try cmd_job_stat.addArgs(&[_]Arg{
        Arg.singleValueOption(
            "jobs",
            'j',
            "Filter Jobs by these Job-IDs",
        ),
        Arg.singleValueOption(
            "users",
            'u',
            "Filter Jobs owned by these list of Users.",
        ),
        Arg.singleValueOption(
            "nodes",
            'N',
            \\Filter Jobs by Nodes.
            \\Only Jobs that run on at least one of the specified Nodes will be shown.
            ,
        ),
        Arg.singleValueOption(
            "accounts",
            'A',
            "Filter Jobs that run under these list of Accounts.",
        ),
        Arg.singleValueOption(
            "partitions",
            'p',
            "Filter Jobs that run under these list of Partitions.",
        ),
        Arg.singleValueOption(
            "unit",
            'U',
            "The unit in which values like Memory are getting displayed.",
        ),
        Arg.booleanOption(
            "all",
            'a',
            \\Additionally show all Steps separately.
            \\By default, only the Job allocation will be shown.
            ,
        ),
        Arg.booleanOption(
            "with-ok",
            null,
            "Also show jobs that are OK and not underutilizing the resources they run on.",
        ),
        Arg.singleValueOption(
            "underutil-threshold",
            'l',
            "Controls the Threshold when Jobs are considered for underutilization.",
        ),
        Arg.singleValueOption(
            "overutil-threshold",
            'o',
            "Controls the Threshold when Jobs are considered for overutilization.",
        ),
        Arg.singleValueOption(
            "min-cpus",
            null,
            "Look only at Jobs that have at least that amount of CPUs allocated.",
        ),
        Arg.singleValueOption(
            "max-cpus",
            null,
            "Look only at Jobs that have at max that amount of CPUs allocated.",
        ),
        Arg.singleValueOption(
            "min-runtime",
            't',
            \\Only show Jobs that have ran longer than this specified value will be shown.
            \\The Format is in Minutes and the default is 5.
            ,
        ),
    });

    try cmd_job.addSubcommand(cmd_job_stat);

    try scli.addSubcommand(cmd_node);
    try scli.addSubcommand(cmd_job);

    const matches = try app.parseProcess();

    if (matches.subcommandMatches("node")) |node_cmd_args| {
        try cmd_node_run(allocator, node_cmd_args);
    }

    if (matches.subcommandMatches("job")) |job_cmd_args| {
        try cmd_job_run(allocator, job_cmd_args);
    }
}
