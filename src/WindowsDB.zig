const std = @import("std");
const windowskv = @import("WindowsKV.zig");
const util = @import("Util.zig");
const value = @import("Value.zig");
const table = @import("Table.zig");
const scanner = @import("Scanner.zig");
const biter = @import("BIter.zig");

const MODE_UPSERT: u16 = 0; // insert or replac
const MODE_UPDATE_ONLY: u16 = 1; // update existing keys
const MODE_INSERT_ONLY: u16 = 2; // only add new keys

pub const WindowsDB = struct {
    Path: [*:0]const u8,
    kv: *windowskv.WindowsKV,
    tables: std.StringHashMap(*table.TableDef),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, fileName: [*:0]const u8, maxPageCount: u64) !WindowsDB {
        var instance = try allocator.create(windowskv.WindowsKV);
        try instance.init(allocator, fileName, maxPageCount);

        return WindowsDB{ .Path = fileName, .kv = instance, .allocator = allocator, .tables = std.StringHashMap(*table.TableDef).init(allocator) };
    }

    pub fn print(self: *WindowsDB) void {
        self.kv.print();
    }
    pub fn deinit(self: *WindowsDB) void {
        var iterator = self.tables.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.*.deinit(self.allocator);
        }
        _ = self.tables.deinit();
        self.kv.deinit();
        self.allocator.destroy(self.kv);
    }

    // get a single row by the primary key
    pub fn dbGet(self: *WindowsDB, tdef: *const table.TableDef, rec: *table.Record) !bool {
        const bCheck = tdef.checkPrimaryKey(rec);
        if (bCheck == false) {
            return table.TableError.PrimaryKeyValueMissing;
        }

        var key = std.ArrayList(u8).init(self.allocator);
        defer key.deinit();
        try rec.encodeKey(tdef.Prefix, &key);

        //std.debug.print("Key: {s}\n", .{key.items});

        const val = self.kv.Get(key.items);
        if (val) |v| {
            //std.debug.print("Reuslt: {s}\n", .{v});
            try rec.decodeValues(v);
            return true;
        } else {
            return false;
        }
    }

    //get Table Define
    pub fn getTableDefFromDB(self: *WindowsDB, name: []const u8) !*table.TableDef {
        var rec = try table.Record.init(self.allocator, table.TDEF_TABLE);
        defer rec.deinit();

        try rec.Set([]const u8, "name", name);
        const ret = try self.dbGet(table.TDEF_TABLE, &rec);
        if (ret == true) {
            const content = try rec.Get("def");
            const parsed = try std.json.parseFromSlice(
                table.TableDef,
                self.allocator,
                content.?.BYTES,
                .{},
            );
            defer parsed.deinit();

            const def = try table.TableDef.copy(self.allocator, parsed.value);
            return def;
        } else {
            return table.TableError.DefinitionMissing;
        }
    }

    // get the table definition by name
    pub fn getTableDef(self: *WindowsDB, name: []const u8) !*table.TableDef {
        const v = self.tables.get(name);
        if (v != null) {
            return v.?;
        } else {
            const defParsed = try self.getTableDefFromDB(name);
            try self.tables.put(name, defParsed);
            return defParsed;
        }
    }

    // add a row to the table
    pub fn dbUpdate(self: *WindowsDB, tdef: *const table.TableDef, rec: *table.Record, mode: u16) !void {
        var bCheck = tdef.checkRecord(rec);
        if (bCheck == false) {
            return table.TableError.ColumnValueMissing;
        }

        bCheck = tdef.checkPrimaryKey(rec);
        if (bCheck == false) {
            return table.TableError.PrimaryKeyValueMissing;
        }

        var key = std.ArrayList(u8).init(self.allocator);
        defer key.deinit();
        try rec.encodeKey(tdef.Prefix, &key);

        var v = std.ArrayList(u8).init(self.allocator);
        defer v.deinit();

        try rec.encodeValues(&v);

        return self.kv.Set(key.items, v.items, mode);
    }

    // delete a record by its primary key
    pub fn Delete(self: *WindowsDB, rec: *table.Record) !bool {
        const bCheck = rec.def.checkPrimaryKey(rec);
        if (bCheck == false) {
            return table.TableError.PrimaryKeyValueMissing;
        }

        var key = std.ArrayList(u8).init(self.allocator);
        defer key.deinit();
        try rec.encodeKey(rec.def.Prefix, &key);

        return self.kv.Delete(key.items);
    }

    //add a record
    pub fn Set(self: *WindowsDB, rec: *table.Record, mode: u16) !void {
        try self.dbUpdate(rec.def, rec, mode);
    }

    pub fn Insert(self: *WindowsDB, rec: *table.Record) !void {
        try self.Set(rec, MODE_INSERT_ONLY);
    }

    pub fn Update(self: *WindowsDB, rec: *table.Record) !void {
        try self.Set(rec, MODE_UPDATE_ONLY);
    }

    pub fn Upsert(self: *WindowsDB, rec: *table.Record) !void {
        try self.Set(rec, MODE_UPSERT);
    }

    pub fn Get(self: *WindowsDB, rec: *table.Record) !bool {
        return self.dbGet(rec.def, rec);
    }

    pub fn Seek(self: *WindowsDB, key1: table.Record, cmp1: biter.OP_CMP, key2: table.Record, cmp2: biter.OP_CMP) !scanner.Scanner {
        var scanner1 = try scanner.Scanner.createScanner(self.allocator, cmp1, cmp2, key1, key2);
        try scanner1.Seek(self.kv);
        return scanner1;
    }

    //add Table
    pub fn AddTable(self: *WindowsDB, tdef: *table.TableDef) !void {
        //tableDefCheck(tdef);

        //check the existing table
        var rtable = try table.Record.init(self.allocator, table.TDEF_TABLE);
        defer rtable.deinit();

        try rtable.Set([]const u8, "name", tdef.Name);
        const ret1 = try self.dbGet(table.TDEF_META, &rtable);
        if (ret1 == true) {
            return table.TableError.TableAlreadyExit;
        }

        std.debug.assert(0 == tdef.Prefix);
        var rMeta = try table.Record.init(self.allocator, table.TDEF_META);
        defer rMeta.deinit();

        tdef.Prefix = table.TABLE_PREFIX_MIN;
        try rMeta.Set([]const u8, "key", "next_prefix");

        const retSearchMeta = try self.dbGet(table.TDEF_META, &rMeta);
        if (retSearchMeta == true) {
            const v = try rMeta.Get("val");
            tdef.Prefix = @intCast(util.U8ArrayToi32(v.?.BYTES));
        }

        tdef.Prefix += 1;
        try rMeta.Set([]const u8, "val", &util.i32ToU8Array(@intCast(tdef.Prefix)));
        try self.dbUpdate(table.TDEF_META, &rMeta, 0);

        // store the definition
        var strTableDef = std.ArrayList(u8).init(self.allocator);
        defer strTableDef.deinit();
        try table.Marshal(&tdef, &strTableDef);

        try rtable.Set([]const u8, "def", strTableDef.items);
        try self.dbUpdate(table.TDEF_TABLE, &rtable, 0);
    }
};
