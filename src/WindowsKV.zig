const std = @import("std");
const node = @import("BNode.zig");
const btree = @import("BTree.zig");
const kvcontext = @import("KVContext.zig");
const wincontext = @import("WinFreeListContext.zig");
const biter = @import("BIter.zig");

pub const WindowsKV = struct {
    context: *kvcontext.KVContext,
    allocator: std.mem.Allocator,
    btree: *btree.BTree,

    pub fn init(self: *WindowsKV, allocator: std.mem.Allocator, fileName: [*:0]const u8, maxPageCount: u64) !void {
        self.allocator = allocator;
        self.context = try kvcontext.createWindowsFreeListContext(allocator, fileName, maxPageCount);
        self.btree = try allocator.create(btree.BTree);

        try self.context.open();
        try self.btree.init1(allocator, self.context);
    }

    pub fn deinit(self: *WindowsKV) void {
        self.context.close() catch {
            std.debug.print("DB Closed Execption", .{});
        };
        self.context.deinit();

        self.btree.deinit();
        self.allocator.destroy(self.context);
        self.allocator.destroy(self.btree);
    }

    pub fn print(self: *WindowsKV) void {
        self.btree.print();
    }

    pub fn Get(self: *WindowsKV, key: []const u8) ?[]u8 {
        return self.btree.Get(key);
    }

    pub fn Set(self: *WindowsKV, key: []const u8, val: []const u8, mode: u16) !void {
        return self.btree.Set(key, val, mode);
    }

    pub fn Delete(self: *WindowsKV, key: []const u8) !bool {
        return self.btree.Delete(key);
    }

    pub fn Seek(self: *WindowsKV, key: []const u8, cmp: biter.OP_CMP) !*biter.BIter {
        return self.btree.Seek(key, cmp);
    }

    pub fn SeekLE(self: *WindowsKV, key: []const u8) !*biter.BIter {
        return self.btree.SeekLE(key);
    }
};
