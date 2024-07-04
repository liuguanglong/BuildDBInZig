const std = @import("std");
const util = @import("Util.zig");
const btree = @import("BTree.zig");

pub const InsertReqest = struct {
    //tree: *btree.BTree,
    //Out
    Added: bool,
    Updated: bool,
    OldValue: std.ArrayList(u8),
    //in
    Key: []const u8,
    Val: []const u8,
    Mode: u16,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, key: []const u8, val: []const u8, mode: u16) !InsertReqest {
        return InsertReqest{ .Key = key, .Val = val, .Mode = mode, .OldValue = std.ArrayList(u8).init(allocator), .Added = false, .Updated = false, .allocator = allocator };
    }

    pub fn deinit(self: *InsertReqest) void {
        self.OldValue.deinit();
    }
};

pub const DeleteRequest = struct {
    //in
    Key: []const u8,
    //out
    OldValue: std.ArrayList(u8),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, key: []const u8) !DeleteRequest {
        return DeleteRequest{ .Key = key, .OldValue = std.ArrayList(u8).init(allocator), .allocator = allocator };
    }

    pub fn deinit(self: *DeleteRequest) void {
        self.OldValue.deinit();
    }
};
