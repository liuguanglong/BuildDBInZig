const std = @import("std");
const windows = @import("std").os.windows;
const mapping = @import("MappingFile.zig");
const kernel32 = windows.kernel32;

test "FileUtil test" {
    var stdout = std.io.getStdOut().writer();
    var info: windows.SYSTEM_INFO = undefined;
    kernel32.GetSystemInfo(&info);
    std.debug.print("PageSize {d}\n", .{info.dwPageSize});

    const filename: [*:0]const u8 = "C:/temp/log1.txt";
    const maxPageCount = 10000; //Max File Size = MaxPageCount * dwPageSize
    var mappingFile = try mapping.MappingFile.init(filename, info.dwPageSize, maxPageCount);
    defer mappingFile.deinit();

    var content = mappingFile.getContent();
    try stdout.print("Content Len:\n{d}\n", .{content.len});

    var filesize = content.len;
    try mappingFile.extendFile(1);
    content = mappingFile.getContent();
    try stdout.print("ExtendFile Content Len:\n{d}\n", .{content.len});
    const new_content = "ffffff";
    std.mem.copyBackwards(u8, content[filesize .. filesize + new_content.len], new_content);

    filesize = content.len;
    try mappingFile.extendFile(1);
    content = mappingFile.getContent();
    try stdout.print("ExtendFile Content Len:\n{d}\n", .{content.len});
    std.mem.copyBackwards(u8, content[filesize .. filesize + new_content.len], new_content);

    try mappingFile.syncFile();
}
