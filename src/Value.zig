const std = @import("std");
const util = @import("Util.zig");

pub const ValueType = enum(u8) {
    ERROR,
    BYTES,
    INT64,
    INT32,
    INT16,
    INT8,
    BOOL,
};

pub const Value = union(enum) {
    BYTES: []u8,
    INT64: i64,
    INT32: i32,
    INT16: i16,
    INT8: i8,
    BOOL: bool,

    pub fn format(
        self: Value,
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        switch (self) {
            .INT16, .INT32, .INT64, .INT8 => |x| try writer.print("{d}", .{x}),
            .BOOL => |x_bool| {
                if (x_bool == true) {
                    try writer.print("true", .{});
                } else {
                    try writer.print("false", .{});
                }
            },
            .BYTES => |x| try writer.print("{s}", .{x}),
        }
    }
};

pub fn deescapeString(allocator: std.mem.Allocator, in: []const u8) ![]u8 {
    var list = std.ArrayList(u8).init(allocator);
    defer list.deinit();

    //std.debug.print("Before dedescapString: {d}", .{in});
    var idx: u16 = 0;
    while (idx < in.len - 1) {
        if (in[idx] == 1) {
            if (in[idx + 1] == 1) {
                try list.append(0x00);
                idx += 2;
            } else if (in[idx + 1] == 2) {
                try list.append(0x01);
                idx += 2;
            } else {
                try list.append(in[idx]);
                idx += 1;
            }
        } else {
            try list.append(in[idx]);
            idx += 1;
        }
    }
    try list.append(in[idx]);

    return list.toOwnedSlice();
}
// Strings are encoded as nul terminated strings,
// escape the nul byte so that strings contain no nul byte.
pub fn escapeString(in: []const u8, list: *std.ArrayList(u8)) !void {
    // const zeroByte: u8 = 0;
    // const oneByte: u8 = 1;
    // const zeros = countBytes(in, zeroByte);
    // const ones = countBytes(in, oneByte);

    // if (zeros + ones == 0) {
    //     try list.appendSlice(in);
    //     return;
    // }

    var idx: u16 = 0;
    while (idx < in.len) {
        if (in[idx] <= 1) {
            try list.append(0x01);
            try list.append(in[idx] + 1);
        } else {
            try list.append(in[idx]);
        }
        idx += 1;
    }
}

fn countBytes(array: []const u8, byte: u8) u16 {
    var count: u16 = 0;
    for (array) |elem| {
        if (elem == byte) {
            count += 1;
        }
    }
    return count;
}
