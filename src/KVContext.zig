const std = @import("std");
const node = @import("BNode.zig");
const memcontext = @import("MemoryContext.zig");
const wincontext = @import("WindowsContext.zig");
const winfreelistcontext = @import("WinFreeListContext.zig");
const biter = @import("BIter.zig");

pub const ContextError = error{
    NewNodeError,
    GetNodeError,
    DuplicateKey,
    LoadDataException,
    DataSaveException,
    PersistSeviceInitException,
    ExtendFileException,
};

pub const KVContext = union(enum) {
    winContext: *wincontext.WindowsContext,
    memContext: *memcontext.MemoryContext,
    //winFreeListContext: *winfreelistcontext.WindowsFreeListContext,

    pub fn deinit(self: KVContext) void {
        switch (self) {
            inline else => |s| s.deinit(),
        }

        //allocator.destroy(self);
    }

    pub fn open(self: KVContext) ContextError!void {
        switch (self) {
            inline else => |s| return s.open(),
        }
    }

    pub fn close(self: KVContext) ContextError!void {
        switch (self) {
            inline else => |s| return s.close(),
        }
    }

    pub fn getRoot(self: KVContext) u64 {
        switch (self) {
            inline else => |s| {
                const root: u64 = s.getRoot();
                return root;
            },
        }
    }

    pub fn setRoot(self: KVContext, ptr: u64) void {
        switch (self) {
            inline else => |s| s.setRoot(ptr),
        }
    }

    pub fn save(self: KVContext) ContextError!void {
        switch (self) {
            inline else => |s| return s.save(),
        }
    }

    pub fn get(self: KVContext, ptr: u64) ContextError!node.BNode {
        switch (self) {
            inline else => |s| return s.get(ptr),
        }
    }

    pub fn del(self: KVContext, ptr: u64) bool {
        switch (self) {
            inline else => |s| return s.del(ptr),
        }
    }

    pub fn new(self: KVContext, bnode: *node.BNode) ContextError!u64 {
        switch (self) {
            inline else => |s| return s.new(bnode),
        }
    }
};

pub fn createWindowsContext(allocator: std.mem.Allocator, fileName: [*:0]const u8, maxPageCount: u64) !*KVContext {
    var t = try allocator.create(wincontext.WindowsContext);
    try t.init(allocator, fileName, maxPageCount);

    const service = try allocator.create(KVContext);
    service.* = @unionInit(KVContext, "winContext", t);
    return service;
}

pub fn createMemoryContext(allocator: std.mem.Allocator) !*KVContext {
    var t = try allocator.create(memcontext.MemoryContext);
    try t.init(allocator);

    const service = try allocator.create(KVContext);
    service.* = @unionInit(KVContext, "memContext", t);
    return service;
}

// pub fn createWindowsFreeListContext(allocator: std.mem.Allocator, fileName: [*:0]const u8, maxPageCount: u64) !*KVContext {
//     var t = try allocator.create(winfreelistcontext.WindowsFreeListContext);
//     try t.init(allocator, fileName, maxPageCount);

//     const service = try allocator.create(KVContext);
//     service.* = @unionInit(KVContext, "winFreeListContext", t);
//     return service;
// }
