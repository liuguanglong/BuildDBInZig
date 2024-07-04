const std = @import("std");
const util = @import("Util.zig");
const value = @import("Value.zig");
const Value = @import("Value.zig").Value;

pub const TableError = error{ KeyColumnMissing, ColumnMissing, DefinitionMissing, ColumnNotFind, ColumnValueMissing, PrimaryKeyValueMissing, TableAlreadyExit };
pub const TABLE_PREFIX_MIN: u16 = 4;

//Record Table Row
pub const Record = struct {
    Vals: std.ArrayList(?Value),
    def: *const TableDef,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, def: *const TableDef) !Record {
        var Vals = std.ArrayList(?Value).init(allocator);
        try Vals.appendNTimes(null, def.Cols.len);

        return Record{ .def = def, .Vals = Vals, .allocator = allocator };
    }

    pub fn deinit(self: *Record) void {
        for (self.Vals.items) |val| {
            if (val) |v| {
                switch (v) {
                    .BYTES => {
                        self.allocator.free(v.BYTES);
                    },
                    else => {},
                }
            }
        }
        self.Vals.deinit();
    }

    pub fn Set(self: *Record, comptime T: type, key: []const u8, val: T) !void {
        const idx = self.GetColumnIndex(key);
        if (idx) |i| {
            switch (T) {
                []const u8 => {
                    if (self.Vals.items[i]) |item| {
                        switch (item) {
                            .BYTES => {
                                self.allocator.free(item.BYTES);
                            },
                            else => {},
                        }
                    }
                    const array = try self.allocator.alloc(u8, val.len);
                    @memcpy(array, val);
                    self.Vals.items[i] = Value{ .BYTES = array };
                },
                i8 => self.Vals.items[i] = Value{ .INT8 = val },
                i16 => self.Vals.items[i] = Value{ .INT16 = val },
                i32 => self.Vals.items[i] = Value{ .INT32 = val },
                i64 => self.Vals.items[i] = Value{ .INT64 = val },
                bool => self.Vals.items[i] = Value{ .BOOL = val },
                else => self.Vals.items[i] = Value{ .BYTES = "Error" },
            }
        } else {
            return TableError.ColumnNotFind;
        }
    }

    pub fn Get(self: *Record, key: []const u8) !?Value {
        const idx = self.GetColumnIndex(key);
        if (idx) |i| {
            return self.Vals.items[i];
        } else {
            return TableError.ColumnNotFind;
        }
    }

    pub fn GetColumnIndex(self: *Record, key: []const u8) ?u16 {
        var idx: u16 = 0;
        while (idx < self.def.Cols.len) {
            const cmp = util.compareArrays(self.def.Cols[idx], key);
            if (cmp == 0)
                return idx;
            idx += 1;
        }
        return null;
    }

    pub fn decodeValues(self: *Record, in: []const u8) !void {
        var pos: u16 = 0;
        var idx: u16 = self.def.PKeys + 1;
        while (idx < self.def.Cols.len) {
            switch (self.def.Types[idx]) {
                value.ValueType.INT8 => {
                    self.Vals.items[idx] = @unionInit(value.Value, "INT8", util.U8ArrayToi8(in[pos]));
                    pos += 1;
                },
                value.ValueType.INT16 => {
                    self.Vals.items[idx] = @unionInit(value.Value, "INT16", util.U8ArrayToi16(in[pos .. pos + 2]));
                    pos += 2;
                },
                value.ValueType.INT32 => {
                    self.Vals.items[idx] = @unionInit(value.Value, "INT32", util.U8ArrayToi32(in[pos .. pos + 4]));
                    pos += 4;
                },
                value.ValueType.INT64 => {
                    self.Vals.items[idx] = @unionInit(value.Value, "INT64", util.U8ArrayToi32(in[pos .. pos + 8]));
                    pos += 8;
                },
                value.ValueType.BOOL => {
                    const bVal = util.U8ArrayToi8(in[pos]);
                    pos += 1;

                    if (bVal == 1) {
                        self.Vals.items[idx] = @unionInit(value.Value, "BOOL", true);
                    } else {
                        self.Vals.items[idx] = @unionInit(value.Value, "BOOL", false);
                    }
                },
                value.ValueType.BYTES => {
                    var end = pos;
                    while (in[end] != 0)
                        end += 1;

                    // const array = try self.allocator.alloc(u8, end - pos);
                    // @memcpy(array, in[pos..end]);
                    // self.Vals.items[idx] = @unionInit(value.Value, "BYTES", array);
                    if (self.Vals.items[idx] != null)
                        self.allocator.free(self.Vals.items[idx].?.BYTES);
                    const ret = try value.deescapeString(self.allocator, in[pos..end]);
                    self.Vals.items[idx] = @unionInit(value.Value, "BYTES", ret);
                    pos = end + 1;
                },
                value.ValueType.ERROR => {
                    std.debug.panic("Column not defined!", .{});
                },
            }
            idx += 1;
        }
    }

    // for primary keys
    pub fn deencodeKey(self: *Record, in: []const u8) !void {
        const pValue: i32 = @intCast(self.def.Prefix);
        const prefix = util.i32ToU8Array(pValue);

        std.debug.assert(std.mem.eql(u8, prefix[0..prefix.len], in[0..prefix.len]));

        var pos: u16 = prefix.len;
        var idx: u16 = 0;
        while (idx <= self.def.PKeys) {
            switch (self.def.Types[idx]) {
                value.ValueType.INT8 => {
                    self.Vals.items[idx] = @unionInit(value.Value, "INT8", util.U8ArrayToi8(in[pos]));
                    pos += 1;
                },
                value.ValueType.INT16 => {
                    self.Vals.items[idx] = @unionInit(value.Value, "INT16", util.U8ArrayToi16(in[pos .. pos + 2]));
                    pos += 2;
                },
                value.ValueType.INT32 => {
                    self.Vals.items[idx] = @unionInit(value.Value, "INT32", util.U8ArrayToi32(in[pos .. pos + 4]));
                    pos += 4;
                },
                value.ValueType.INT64 => {
                    self.Vals.items[idx] = @unionInit(value.Value, "INT64", util.U8ArrayToi32(in[pos .. pos + 8]));
                    pos += 8;
                },
                value.ValueType.BOOL => {
                    const bVal = util.U8ArrayToi8(in[pos]);
                    pos += 1;

                    if (bVal == 1) {
                        self.Vals.items[idx] = @unionInit(value.Value, "BOOL", true);
                    } else {
                        self.Vals.items[idx] = @unionInit(value.Value, "BOOL", false);
                    }
                },
                value.ValueType.BYTES => {
                    var end = pos;
                    while (in[end] != 0)
                        end += 1;

                    if (self.Vals.items[idx] != null)
                        self.allocator.free(self.Vals.items[idx].?.BYTES);
                    const ret = try value.deescapeString(self.allocator, in[pos..end]);
                    self.Vals.items[idx] = @unionInit(value.Value, "BYTES", ret);
                    pos = end + 1;
                },
                value.ValueType.ERROR => {
                    std.debug.panic("Column not defined!", .{});
                },
            }
            idx += 1;
        }
    }

    // order-preserving encoding
    pub fn encodeValues(self: *Record, list: *std.ArrayList(u8)) !void {
        //var list = std.ArrayList(u8).init(allocator);
        var idx: u16 = self.def.PKeys + 1;
        while (idx < self.def.Cols.len) {
            switch (self.def.Types[idx]) {
                value.ValueType.INT8 => try list.appendSlice(&util.i8ToU8Array(self.Vals.items[idx].?.INT8)),
                value.ValueType.INT16 => try list.appendSlice(&util.i16ToU8Array(self.Vals.items[idx].?.INT16)),
                value.ValueType.INT32 => try list.appendSlice(&util.i32ToU8Array(self.Vals.items[idx].?.INT32)),
                value.ValueType.INT64 => try list.appendSlice(&util.i64ToU8Array(self.Vals.items[idx].?.INT64)),
                value.ValueType.BOOL => {
                    if (self.Vals.items[idx].?.BOOL == true) {
                        try list.appendSlice(&util.i8ToU8Array(1));
                    } else {
                        try list.appendSlice(&util.i8ToU8Array(0));
                    }
                },
                value.ValueType.BYTES => {
                    try value.escapeString(self.Vals.items[idx].?.BYTES, list);
                    //try list.appendSlice(v);
                    try list.append(0);
                },
                value.ValueType.ERROR => {
                    std.debug.panic("Column not defined!", .{});
                },
            }
            idx += 1;
        }
    }

    // order-preserving encoding
    pub fn encodeKeys(self: *Record, list: *std.ArrayList(u8)) !void {
        var idx: u16 = 0;

        while (idx <= self.def.PKeys) {
            switch (self.def.Types[idx]) {
                value.ValueType.INT8 => try list.appendSlice(&util.i8ToU8Array(self.Vals.items[idx].?.INT8)),
                value.ValueType.INT16 => try list.appendSlice(&util.i16ToU8Array(self.Vals.items[idx].?.INT16)),
                value.ValueType.INT32 => try list.appendSlice(&util.i32ToU8Array(self.Vals.items[idx].?.INT32)),
                value.ValueType.INT64 => try list.appendSlice(&util.i64ToU8Array(self.Vals.items[idx].?.INT64)),
                value.ValueType.BOOL => {
                    if (self.Vals.items[idx].?.BOOL == true) {
                        try list.appendSlice(&util.i8ToU8Array(1));
                    } else {
                        try list.appendSlice(&util.i8ToU8Array(0));
                    }
                },
                value.ValueType.BYTES => {
                    try value.escapeString(self.Vals.items[idx].?.BYTES, list);
                    //try list.appendSlice(v);
                    try list.append(0);
                },
                value.ValueType.ERROR => {
                    std.debug.panic("Column not defined!", .{});
                },
            }
            idx += 1;
        }
    }

    // for primary keys
    pub fn encodeKey(self: *Record, prefix: u32, list: *std.ArrayList(u8)) !void {
        const pValue: i32 = @intCast(prefix);
        try list.appendSlice(&util.i32ToU8Array(pValue));
        try self.encodeKeys(list);
    }

    pub fn format(
        self: Record,
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try writer.print("Record Content:\n|", .{});
        var iCols: u16 = 0;
        while (iCols < self.def.Cols.len) {
            try writer.print("{s}|", .{self.def.Cols[iCols]});
            iCols += 1;
        }
        try writer.print("\n|", .{});
        iCols = 0;
        while (iCols < self.def.Cols.len) {
            try writer.print("{any}|", .{self.Vals.items[iCols]});
            iCols += 1;
        }
    }
};

pub const TableDef = struct {
    Name: []const u8,
    Types: []const value.ValueType,
    Cols: []const []const u8,
    PKeys: u16,
    Prefix: u32,
    Indexes: []const []const []const u8,
    IndexPrefixes: []const u32,

    pub fn format(
        self: TableDef,
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try writer.print("Table Definition:\n|", .{});
        var iCols: u16 = 0;
        while (iCols < self.Cols.len) {
            try writer.print("{s}|", .{self.Cols[iCols]});
            iCols += 1;
        }
        try writer.print("\n|", .{});
        iCols = 0;
        while (iCols < self.Types.len) {
            try writer.print("{}|", .{self.Types[iCols]});
            iCols += 1;
        }

        try writer.print("\nPrimary Keys\n", .{});
        iCols = 0;
        while (iCols <= self.PKeys) {
            try writer.print("{s}|", .{self.Cols[iCols]});
            iCols += 1;
        }

        try writer.print("\nIndexes\n", .{});
        for (self.Indexes) |index| {
            for (index) |col| {
                try writer.print("{s}|", .{col});
            }
            try writer.print("\n", .{});
        }
    }

    pub fn deinit(self: *TableDef, allocator: std.mem.Allocator) void {
        for (self.Cols) |col| {
            allocator.free(col);
        }

        for (self.Indexes) |index| {
            for (index) |item| {
                allocator.free(item);
            }
            allocator.free(index);
        }

        allocator.free(self.Indexes);
        allocator.free(self.IndexPrefixes);
        allocator.free(self.Name);
        allocator.free(self.Cols);
        allocator.free(self.Types);

        allocator.destroy(self);
    }

    // check primaykey
    pub fn checkPrimaryKey(self: *const TableDef, rec: *Record) bool {
        var idx: u16 = 0;
        while (idx <= self.PKeys) {
            if (rec.Vals.items[idx] == null) {
                std.debug.print("Check Primary Key Error. Col:{d} is null\n", .{idx});
                return false;
            }
            idx += 1;
        }
        return true;
    }

    pub fn colIndex(self: *const TableDef, col: []const u8) !usize {
        for (self.Cols, 0..) |colDef, i| {
            if (util.compareArrays(colDef, col) == 0) {
                return i;
            }
        }
        return TableError.ColumnNotFind;
    }

    // check Indexes
    pub fn checkIndexes(self: *const TableDef, rec: *Record) bool {
        for (self.Indexes) |Index| {
            for (Index) |col| {
                const idx = self.colIndex(col) catch {
                    std.debug.panic("Col not find in Table Definition!{s}", .{col});
                };
                if (rec.Vals.items[idx] == null) {
                    std.debug.print("Check Indexes Error. Col:{s} is null!\n", .{col});
                    return false;
                }
            }
        }
        return true;
    }

    // check record
    pub fn checkRecord(self: *const TableDef, rec: *Record) bool {
        var idx: u16 = self.PKeys + 1;
        while (idx < self.Cols.len) {
            if (rec.Vals.items[idx] == null) {
                std.debug.print("Check Error. Idx;{d}", .{idx});
                return false;
            }
            idx += 1;
        }
        return true;
    }

    // check indexes and and primary key to indexes
    pub fn fixIndexKeys(self: *TableDef, allocator: std.mem.Allocator) !void {
        var indexes = std.ArrayList([]const []const u8).init(allocator);
        defer indexes.deinit();

        //Add Primary Key To Indexes
        for (self.Indexes) |index| {
            var items = std.ArrayList([]const u8).init(allocator);
            defer items.deinit();

            var idx: u16 = 0;
            while (idx <= self.PKeys) {
                try items.append(self.Cols[idx]);
                idx += 1;
            }
            try items.appendSlice(index);

            const c2 = try items.toOwnedSlice();
            try indexes.append(c2);
        }

        self.Indexes = try indexes.toOwnedSlice();
    }
};

pub const TDEF_META = &TableDef{
    .Prefix = 1,
    .Name = "@meta",
    .Types = &.{ value.ValueType.BYTES, value.ValueType.BYTES },
    .Cols = &.{ "key", "val" },
    .PKeys = 0,
    .Indexes = &.{},
    .IndexPrefixes = &.{},
};

pub const TDEF_TABLE = &TableDef{
    .Prefix = 2,
    .Name = "@table",
    .Types = &.{ value.ValueType.BYTES, value.ValueType.BYTES },
    .Cols = &.{ "name", "def" },
    .PKeys = 0,
    .Indexes = &.{},
    .IndexPrefixes = &.{},
};

pub fn copy(allocator: std.mem.Allocator, def: *const TableDef) !*TableDef {
    var name = std.ArrayList(u8).init(allocator);
    defer name.deinit();
    try name.appendSlice(def.Name);

    var cols = std.ArrayList([]const u8).init(allocator);
    defer cols.deinit();
    for (def.Cols) |col| {
        var c = std.ArrayList(u8).init(allocator);
        try c.appendSlice(col);
        const c1 = try c.toOwnedSlice();
        try cols.append(c1);
    }

    var types = std.ArrayList(value.ValueType).init(allocator);
    defer types.deinit();
    for (def.Types) |t| {
        try types.append(t);
    }

    var newObj = try allocator.create(TableDef);
    newObj.Name = try name.toOwnedSlice();
    newObj.Cols = try cols.toOwnedSlice();
    newObj.Types = try types.toOwnedSlice();
    newObj.Prefix = def.Prefix;
    newObj.PKeys = def.PKeys;

    //Copy Indexes
    var indexs = std.ArrayList([][]const u8).init(allocator);
    //defer indexs.deinit();
    for (def.Indexes) |index| {
        var items = std.ArrayList([]const u8).init(allocator);
        for (index) |col| {
            var c = std.ArrayList(u8).init(allocator);
            try c.appendSlice(col);
            const c1 = try c.toOwnedSlice();
            try items.append(c1);
        }
        const c2 = try items.toOwnedSlice();
        try indexs.append(c2);
    }
    newObj.Indexes = try indexs.toOwnedSlice();

    var prefixes = std.ArrayList(u32).init(allocator);
    try prefixes.appendSlice(def.IndexPrefixes);
    newObj.IndexPrefixes = try prefixes.toOwnedSlice();

    return newObj;
}

pub fn Marshal(allocator: std.mem.Allocator, def: *const TableDef, out: *std.ArrayList(u8)) !void {
    var indexes = std.ArrayList([]const []const u8).init(allocator);
    defer indexes.deinit();

    var newDef = try copy(allocator, def);
    defer newDef.deinit(allocator);

    //Add Primary Key To Indexes
    for (newDef.Indexes) |index| {
        var items = std.ArrayList([]const u8).init(allocator);
        var idx: u16 = 0;
        while (idx <= newDef.PKeys) {
            var c = std.ArrayList(u8).init(allocator);
            try c.appendSlice(newDef.Cols[idx]);
            const c1 = try c.toOwnedSlice();
            try items.append(c1);
            idx += 1;
        }

        for (index) |col| {
            var c = std.ArrayList(u8).init(allocator);
            try c.appendSlice(col);
            const c1 = try c.toOwnedSlice();
            try items.append(c1);
        }
        //try items.appendSlice(index);
        const c2 = try items.toOwnedSlice();
        try indexes.append(c2);
    }

    for (newDef.Indexes) |index| {
        for (index) |item| {
            allocator.free(item);
        }
        allocator.free(index);
    }

    allocator.free(newDef.Indexes);

    newDef.Indexes = try indexes.toOwnedSlice();
    try std.json.stringify(newDef, .{}, out.writer());
}
