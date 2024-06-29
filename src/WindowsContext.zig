const std = @import("std");
const node = @import("BNode.zig");
const mapping = @import("MappingFile.zig");
const context = @import("KVContext.zig");

const DB_SIG = "BuildYourOwnDB22";

pub const WindowsContext = struct {
    root: u64,
    file: mapping.MappingFile,
    pageflushed: u64,
    allocator: std.mem.Allocator,
    tempPages: std.ArrayList(*node.BNode),
    fileName: [*:0]const u8,
    maxPageCount: u64,

    pub fn init(self: *WindowsContext, allocator: std.mem.Allocator, fileName: [*:0]const u8, maxPageCount: u64) !void {
        const nodelmax = node.HEADER + 8 + 2 + 4 + node.BTREE_MAX_KEY_SIZE + node.BTREE_MAX_VALUE_SIZE;
        std.debug.assert(nodelmax <= node.BTREE_PAGE_SIZE);
        self.tempPages = std.ArrayList(*node.BNode).init(allocator);
        self.file = mapping.MappingFile.init(fileName, node.BTREE_PAGE_SIZE, maxPageCount) catch {
            return context.ContextError.PersistSeviceInitException;
        };
        self.allocator = allocator;
    }

    pub fn deinit(self: *WindowsContext) void {
        self.file.syncFile() catch unreachable;
        self.clearTempPages();
        _ = self.tempPages.deinit();
        _ = self.file.deinit();

        self.allocator.destroy(self);
    }

    pub fn open(self: *WindowsContext) context.ContextError!void {
        try self.masterLoad();
    }

    pub fn getRoot(self: *WindowsContext) u64 {
        return self.root;
    }

    pub fn setRoot(self: *WindowsContext, ptr: u64) void {
        self.root = ptr;
    }

    // the master page format.
    // it contains the pointer to the root and other important bits.
    // | sig | node_root | page_used |
    // | 16B | 8B | 8B |
    fn masterLoad(self: *WindowsContext) context.ContextError!void {
        const content = self.file.getContent();
        if (content.len == 0) {
            self.pageflushed = 1;
            self.file.extendFile(2) catch {
                return context.ContextError.ExtendFileException;
            };
            return;
        }

        var root: u64 = 0;
        var pos: u16 = 16;
        root |= @as(u64, content[pos + 0]) << 56;
        root |= @as(u64, content[pos + 1]) << 48;
        root |= @as(u64, content[pos + 2]) << 40;
        root |= @as(u64, content[pos + 3]) << 32;
        root |= @as(u64, content[pos + 4]) << 24;
        root |= @as(u64, content[pos + 5]) << 16;
        root |= @as(u64, content[pos + 6]) << 8;
        root |= @as(u64, content[pos + 7]);

        var used: u64 = 0;
        pos = 24;
        used |= @as(u64, content[pos + 0]) << 56;
        used |= @as(u64, content[pos + 1]) << 48;
        used |= @as(u64, content[pos + 2]) << 40;
        used |= @as(u64, content[pos + 3]) << 32;
        used |= @as(u64, content[pos + 4]) << 24;
        used |= @as(u64, content[pos + 5]) << 16;
        used |= @as(u64, content[pos + 6]) << 8;
        used |= @as(u64, content[pos + 7]);

        // verify the page
        if (std.mem.eql(u8, DB_SIG, content[0..16]) == false) {
            return context.ContextError.LoadDataException;
        }

        var bad: bool = !(1 <= used and used <= (content.len / node.BTREE_PAGE_SIZE));
        bad = bad or !(0 <= root and root < used);
        if (bad == true) {
            return context.ContextError.LoadDataException;
        }

        self.root = root;
        self.pageflushed = used;
    }

    // extend the file to at least `npages`.
    fn extendFile(self: *WindowsContext, npages: i64) context.ContextError!void {
        var filePages: i64 = @divTrunc(self.file.fileSize, node.BTREE_PAGE_SIZE);
        if (filePages >= npages)
            return;

        var nPageExtend: i64 = 0;
        while (filePages < npages) {
            var inc = @divTrunc(filePages, 8);
            if (inc < 1) {
                inc = 1;
            }
            nPageExtend += inc;
            filePages += inc;
        }

        self.file.extendFile(nPageExtend) catch {
            return context.ContextError.DataSaveException;
        };
    }

    pub fn close(self: *WindowsContext) context.ContextError!void {
        try self.masterStore();
        self.file.syncFile() catch {
            return context.ContextError.DataSaveException;
        };
    }

    // update the master page. it must be atomic.
    fn masterStore(self: *WindowsContext) context.ContextError!void {
        var data: [32]u8 = [_]u8{0} ** 32;
        std.mem.copyForwards(u8, &data, DB_SIG);

        var pos: u16 = 16;
        var value: u64 = self.root;
        data[pos + 0] = @intCast((value >> 56) & 0xFF);
        data[pos + 1] = @intCast((value >> 48) & 0xFF);
        data[pos + 2] = @intCast((value >> 40) & 0xFF);
        data[pos + 3] = @intCast((value >> 32) & 0xFF);
        data[pos + 4] = @intCast((value >> 24) & 0xFF);
        data[pos + 5] = @intCast((value >> 16) & 0xFF);
        data[pos + 6] = @intCast((value >> 8) & 0xFF);
        data[pos + 7] = @intCast(value & 0xFF);

        pos = 24;
        value = self.pageflushed;
        data[pos + 0] = @intCast((value >> 56) & 0xFF);
        data[pos + 1] = @intCast((value >> 48) & 0xFF);
        data[pos + 2] = @intCast((value >> 40) & 0xFF);
        data[pos + 3] = @intCast((value >> 32) & 0xFF);
        data[pos + 4] = @intCast((value >> 24) & 0xFF);
        data[pos + 5] = @intCast((value >> 16) & 0xFF);
        data[pos + 6] = @intCast((value >> 8) & 0xFF);
        data[pos + 7] = @intCast(value & 0xFF);

        std.mem.copyForwards(u8, self.file.getContent(), &data);
    }

    pub fn save(self: *WindowsContext) context.ContextError!void {
        try self.writePages();
        try self.syncPages();
    }

    fn writePages(self: *WindowsContext) context.ContextError!void {
        const count = self.tempPages.items.len;
        const nPages: i64 = @intCast(self.pageflushed + count);
        try self.extendFile(nPages);

        for (self.tempPages.items, 0..) |_, index| {
            const ptr = self.pageflushed + index;
            const pdest = self.getFilePtr(ptr) catch unreachable;
            var ptdData = self.tempPages.items[index].getdata();
            std.mem.copyForwards(u8, pdest[0..node.BTREE_PAGE_SIZE], ptdData[0..node.BTREE_PAGE_SIZE]);
        }
    }

    fn getFilePtr(self: *WindowsContext, ptr: u64) context.ContextError![]u8 {
        const offset = ptr * node.BTREE_PAGE_SIZE;
        const content = self.file.getContent();
        return content[offset .. offset + node.BTREE_PAGE_SIZE];
    }

    fn syncPages(self: *WindowsContext) context.ContextError!void {
        self.file.syncFile() catch {
            return context.ContextError.DataSaveException;
        };

        self.pageflushed += self.tempPages.items.len;
        self.clearTempPages();
        self.tempPages.clearRetainingCapacity();

        try self.masterStore();
        self.file.syncFile() catch {
            return context.ContextError.DataSaveException;
        };
    }

    fn clearTempPages(self: *WindowsContext) void {
        const len = self.tempPages.items.len;
        for (0..len) |_| {
            const item = self.tempPages.pop();
            const ptr = item.data.Single;
            self.allocator.destroy(item);
            self.allocator.free(ptr);
        }
    }

    pub fn get(self: *WindowsContext, ptr: u64) context.ContextError!node.BNode {
        if (ptr > self.pageflushed) {
            std.debug.panic("Get Page Exception!", .{});
        }
        const offset = ptr * node.BTREE_PAGE_SIZE;
        const content = self.file.getContent();
        return node.BNode.initSigleCapacity(content[offset .. offset + node.BTREE_PAGE_SIZE][0..node.BTREE_PAGE_SIZE]);
    }

    pub fn del(_: *WindowsContext, _: u64) bool {
        return true;
    }

    pub fn new(self: *WindowsContext, bnode: *node.BNode) context.ContextError!u64 {
        const ptrNodeData = bnode.getdata();
        var ptrData = self.allocator.alloc(u8, node.BTREE_PAGE_SIZE) catch {
            unreachable;
        };
        const newnode = self.allocator.create(node.BNode) catch {
            unreachable;
        };
        newnode.* = node.BNode{ .data = @unionInit(node.BNodeData, "Single", ptrData[0..node.BTREE_PAGE_SIZE]) };
        @memcpy(ptrData[0..node.BTREE_PAGE_SIZE], ptrNodeData[0..node.BTREE_PAGE_SIZE]);

        const ptr = self.pageflushed + self.tempPages.items.len;
        self.tempPages.append(newnode) catch unreachable;

        return ptr;
    }
};
