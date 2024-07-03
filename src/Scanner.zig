const std = @import("std");
const node = @import("BNode.zig");
const btree = @import("BTree.zig");
const biter = @import("BIter.zig");
const table = @import("Table.zig");
const WindowsKV = @import("WindowsKV.zig");

pub const ScannerError = error{ BadArrange, RangeKeyError, KeyError };

// the iterator for range queries
pub const Scanner = struct {
    // the range, from Key1 to Key2
    Cmp1: biter.OP_CMP,
    Cmp2: biter.OP_CMP,
    Key1: *table.Record,
    Key2: *table.Record,
    //Internal
    tdef: *const table.TableDef,
    iter: *biter.BIter,
    keyEnd: std.ArrayList(u8),
    keyStart: std.ArrayList(u8),
    allocator: *std.mem.Allocator,

    pub fn createScanner(allocator: std.mem.Allocator, cmp1: biter.OP_CMP, cmp2: biter.OP_CMP, key1: *table.Record, key2: *table.Record) !*Scanner {
        var scanner = try allocator.create(Scanner);

        scanner.keyEnd = std.ArrayList(u8).init(allocator);
        scanner.keyStart = std.ArrayList(u8).init(allocator);
        scanner.Cmp1 = cmp1;
        scanner.Cmp2 = cmp2;
        scanner.Key1 = key1;
        scanner.Key2 = key2;
        scanner.tdef = key1.def;
        scanner.iter = undefined;

        return scanner;
    }

    pub fn deinit(self: *Scanner) void {
        self.keyEnd.deinit();
        self.keyStart.deinit();
        if (self.iter != undefined)
            self.iter.deinit();
        //self.allocator.destroy(self);
    }

    pub fn Seek(self: *Scanner, db: *WindowsKV.WindowsKV) !void {
        // sanity checks
        if (@intFromEnum(self.Cmp1) > 0 and @intFromEnum(self.Cmp2) < 0) {} else if (@intFromEnum(self.Cmp2) > 0 and @intFromEnum(self.Cmp1) < 0) {} else {
            return ScannerError.BadArrange;
        }

        const bCheck1 = self.tdef.checkPrimaryKey(self.Key1);
        if (bCheck1 == false) {
            return ScannerError.KeyError;
        }
        const bCheck2 = self.tdef.checkPrimaryKey(self.Key2);
        if (bCheck2 == false) {
            return ScannerError.KeyError;
        }

        try self.Key1.encodeKey(self.tdef.Prefix, &self.keyStart);
        try self.Key2.encodeKey(self.tdef.Prefix, &self.keyEnd);

        self.iter = try db.Seek(self.keyStart.items, self.Cmp1);
    }

    pub fn Valid(self: *Scanner) bool {
        if (self.iter == undefined) {
            return false;
        }
        const kv = self.iter.Deref();
        return biter.cmpOK(kv.key, self.keyEnd.items, self.Cmp2);
    }

    pub fn Deref(self: *Scanner, rec: *table.Record) !void {
        const kv = self.iter.Deref();
        if (kv.val.len > 0) {
            try rec.deencodeKey(kv.key);
            try rec.decodeValues(kv.val);
        }
    }

    pub fn Next(self: *Scanner) void {
        std.debug.assert(self.Valid());
        if (@intFromEnum(self.Cmp1) > 0) {
            _ = self.iter.Next();
        } else {
            _ = self.iter.Prev();
        }
    }
};
