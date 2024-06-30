const std = @import("std");
const windb = @import("WindowsDB.zig");
const util = @import("Util.zig");
const value = @import("Value.zig");
const table = @import("Table.zig");
const biter = @import("BIter.zig");
const scanner = @import("Scanner.zig");

const testing = std.testing;

test "Test Seek" {
    std.debug.print("\nTest Seek Record\n", .{});
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("TEST FAIL");
    }

    //const allocator = std.heap.page_allocator;
    const filename: [:0]const u8 = "c:/temp/winkvStore2.data";
    var instance = try windb.WindowsDB.init(allocator, filename, 20000);
    defer instance.deinit();

    const def = try instance.getTableDef("person");
    std.debug.print("Table Define: {} \n", .{def.*});

    var r1 = try table.Record.init(allocator, def);
    defer r1.deinit();

    var r2 = try table.Record.init(allocator, def);
    defer r2.deinit();

    try r1.Set([]const u8, "id", "180");
    try r2.Set([]const u8, "id", "200");

    var cursor = try scanner.Scanner.createScanner(allocator, biter.OP_CMP.CMP_GT, biter.OP_CMP.CMP_LT, &r1, &r2);
    defer allocator.destroy(cursor);
    defer cursor.deinit();

    try cursor.Seek(instance.kv);

    var r3 = try table.Record.init(allocator, def);
    defer r3.deinit();
    while (cursor.Valid()) {
        try cursor.Deref(&r3);
        std.debug.print("{}\n", .{r3});
        cursor.Next();
    }
}

// test "Test Delete" {
//     std.debug.print("\nTest Delete Record\n", .{});
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//     defer {
//         const deinit_status = gpa.deinit();
//         if (deinit_status == .leak) @panic("TEST FAIL");
//     }

//     //const allocator = std.heap.page_allocator;
//     const filename: [:0]const u8 = "c:/temp/winkvStore2.data";
//     var instance = try windb.WindowsDB.init(allocator, filename, 20000);
//     defer instance.deinit();

//     const def = try instance.getTableDef("person");
//     std.debug.print("Table Define: {} \n", .{def.*});

//     var r1 = try table.Record.init(allocator, def);
//     defer r1.deinit();

//     try r1.Set([]const u8, "id", "1");
//     const ret1 = try instance.Delete(&r1);
//     std.debug.print("Delete Result:{}\n", .{ret1});
// }

// test "Test Insert Record" {
//     std.debug.print("\nTest Insert Record\n", .{});

//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//     defer {
//         const deinit_status = gpa.deinit();
//         if (deinit_status == .leak) @panic("TEST FAIL");
//     }

//     const filename: [:0]const u8 = "c:/temp/winkvStore2.data";
//     var instance = try windb.WindowsDB.init(allocator, filename, 20000000);
//     defer instance.deinit();

//     //instance.print();

//     const def = try instance.getTableDef("person");
//     std.debug.print("Table Define: {} \n", .{def.*});

//     // var r = try table.Record.init(allocator, def);
//     // defer r.deinit();

//     // for (500..1000) |idx| {
//     //     const id = try std.fmt.allocPrint(
//     //         allocator,
//     //         "{d}",
//     //         .{idx},
//     //     );
//     //     defer allocator.free(id);

//     //     const name = try std.fmt.allocPrint(
//     //         allocator,
//     //         "bob{d}",
//     //         .{idx},
//     //     );
//     //     defer allocator.free(name);
//     //     try r.Set([]const u8, "id", id);
//     //     try r.Set([]const u8, "name", name);
//     //     try r.Set([]const u8, "address", "Montrel Canada H9T 1R5");
//     //     try r.Set(i16, "age", 20);
//     //     try r.Set(bool, "married", false);

//     //     try instance.Insert(&r);
//     // }

//     var r1 = try table.Record.init(allocator, def);
//     defer r1.deinit();

//     try r1.Set([]const u8, "id", "488");
//     const ret1 = try instance.Get(&r1);
//     if (ret1 == true) {
//         std.debug.print("Row:{}\n", .{r1});
//     }
// }

// test "Test Insert min_preifx to Meta" {
//     std.debug.print("\nInsert min_preifx to Meta\n", .{});

//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//     defer {
//         const deinit_status = gpa.deinit();
//         if (deinit_status == .leak) @panic("TEST FAIL");
//     }

//     const filename: [:0]const u8 = "c:/temp/winkvStore2.data";
//     var instance = try windb.WindowsDB.init(allocator, filename, 2000);
//     defer instance.deinit();

//     var r = try table.Record.init(allocator, table.TDEF_META);
//     defer r.deinit();

//     const minPreix: u32 = 3;
//     const valContent = util.i32ToU8Array(minPreix);
//     std.debug.print("Val Content:{s}\n", .{valContent});

//     try r.Set([]const u8, "key", "next_prefix");
//     try r.Set([]const u8, "val", &valContent);
//     std.debug.print("Record Conent: {} \n", .{r});

//     _ = try instance.dbUpdate(table.TDEF_META, &r, 0);
//     var r1 = try table.Record.init(allocator, table.TDEF_META);
//     defer r1.deinit();

//     try r1.Set([]const u8, "key", "next_prefix");
//     const ret1 = try instance.dbGet(table.TDEF_META, &r1);
//     if (ret1 == true) {
//         const v = try r1.Get("val");
//         const min_next = util.U8ArrayToi32(v.?.BYTES);
//         std.debug.print("next_prefix:{d}\n", .{min_next});
//     }
// }

// test "Test Insert Table" {
//     std.debug.print("\nTest Insert Table\n", .{});

//     var testTable = table.TableDef{
//         .Prefix = 0,
//         .Name = "person",
//         .Types = &.{ value.ValueType.BYTES, value.ValueType.BYTES, value.ValueType.BYTES, value.ValueType.INT16, value.ValueType.BOOL },
//         .Cols = &.{ "id", "name", "address", "age", "married" },
//         .PKeys = 0,
//     };

//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//     defer {
//         const deinit_status = gpa.deinit();
//         if (deinit_status == .leak) @panic("TEST FAIL");
//     }

//     //const allocator = std.heap.page_allocator;
//     const filename: [:0]const u8 = "c:/temp/winkvStore2.data";
//     var instance = try windb.WindowsDB.init(allocator, filename, 2000);
//     defer instance.deinit();

//     try instance.AddTable(&testTable);

//     const def = try instance.getTableDef(testTable.Name);
//     std.debug.print("Table Define: {}", .{def.*});
// }
