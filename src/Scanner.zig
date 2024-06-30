const std = @import("std");
const node = @import("BNode.zig");
const btree = @import("BTree.zig");
const biter = @import("BIter.zig");
const table = @import("Table.zig");
const kvstore = @import("WindowsDB.zig");

pub const ScannerError = error{ BadArrange, RangeKeyError };

// the iterator for range queries
pub const Scanner = struct {
    // the range, from Key1 to Key2
    Cmp1: biter.OP_CMP,
    Cmp2: biter.OP_CMP,
    Key1: table.Record,
    Key2: table.Record,
    //Internal
    tdef: *table.TableDef,
    iter: *biter.BITer,
    keyEnd: std.ArrayList(u8),
    keyStart: std.ArrayList(u8),
    allocator: *std.mem.Allocator,

    pub fn createScanner(allocator: std.mem.Allocator, cmp1: biter.OP_CMP, cmp2: biter.OP_CMP, key1: table.Record, key2: table.Record) !Scanner {
        var scanner = try allocator.create(Scanner);

        scanner.keyEnd = try std.ArrayList(u8).init(allocator);
        scanner.keyStart = try std.ArrayList(u8).init(allocator);
        scanner.Cmp1 = cmp1;
        scanner.Cmp2 = cmp2;
        scanner.key1 = key1;
        scanner.key2 = key2;
        scanner.iter = undefined;

        return scanner;
    }

    pub fn deinit(self: *Scanner) void {
        self.keyEnd.deinit();
        self.keyStart.deinit();

        self.allocator.destroy(self);
    }

    pub fn Seek(self: *Scanner, db: *kvstore.KVStore) !void {
        // sanity checks
        if (self.Cmp1 > 0 and self.Cmp2 < 0) {} else if (self.Cmp2 > 0 and self.Cmp1 < 0) {} else {
            return ScannerError.BadArrange;
        }

        const bCheck1 = self.tdef.checkPrimaryKey(self.key1);
        if (bCheck1 == false) {
            return ScannerError.KeyError;
        }
        const bCheck2 = self.tdef.checkPrimaryKey(self.key2);
        if (bCheck2 == false) {
            return ScannerError.KeyError;
        }

        try self.key1.encodeKey(self.tdef, self.keyStart);
        try self.key2.encodeKey(self.tdef, self.keyEnd);

        self.iter = db.Seek(self.keyStart.items, self.Cmp1);
    }

    pub fn Valid(self: *Scanner) bool {
        if (self.iter == undefined) {
            return false;
        }
        const kv = self.iter.Deref();
        return self.iter.cmpOK(kv.key, self.keyEnd.items, self.Cmp2);
    }

    pub fn Next(self: *Scanner) void {
        std.debug.assert(self.Valid());
        if (self.Cmp1 > 0) {
            _ = self.iter.Next();
        } else {
            _ = self.iter.Prev();
        }
    }
};
