const std = @import("std");
const node = @import("BNode.zig");
const mapping = @import("MappingFile.zig");
const freelist = @import("FreeList.zig");
const context = @import("KVContext.zig");

const DB_SIG = "BuildYourOwnDB22";

pub const WindowsFreeListContext = struct {
    root: u64,
    fileName: [*:0]const u8,
    file: mapping.MappingFile,
    allocator: std.mem.Allocator,
    maxPageCount: u64,

    freepages: freelist.FreeList,
    pageflushed: u64, // database size in number of pages
    nfreelist: u16, //number of pages taken from the free list
    nappend: u16, //number of pages to be appended
    // newly allocated or deallocated pages keyed by the pointer.
    // nil value denotes a deallocated page.
    updates: std.AutoHashMap(u64, ?*node.BNode),

    pub fn init(self: *WindowsFreeListContext, allocator: std.mem.Allocator, fileName: [*:0]const u8, maxPageCount: u64) !void {
        const nodelmax = node.HEADER + 8 + 2 + 4 + node.BTREE_MAX_KEY_SIZE + node.BTREE_MAX_VALUE_SIZE;

        std.debug.assert(nodelmax <= node.BTREE_PAGE_SIZE);
        self.updates = std.AutoHashMap(u64, ?*node.BNode).init(allocator);
        self.file = mapping.MappingFile.init(fileName, node.BTREE_PAGE_SIZE, maxPageCount) catch {
            return context.ContextError.PersistSeviceInitException;
        };
        self.allocator = allocator;
        self.freepages = freelist.FreeList.init(self, allocator, 0);
    }

    pub fn deinit(self: *WindowsFreeListContext) void {
        self.file.syncFile() catch unreachable;

        _ = self.file.deinit();
        self.clearUpdatePages();
        _ = self.updates.deinit();

        self.allocator.destroy(self);
    }

    pub fn open(self: *WindowsFreeListContext) context.ContextError!void {
        try self.masterLoad();
    }

    pub fn getRoot(self: *WindowsFreeListContext) u64 {
        return self.root;
    }

    pub fn setRoot(self: *WindowsFreeListContext, ptr: u64) void {
        self.root = ptr;
    }

    // the master page format.
    // it contains the pointer to the root and other important bits.
    // | sig | btree_root | page_used |
    // | 16B | 8B | 8B |
    fn masterLoad(self: *WindowsFreeListContext) context.ContextError!void {
        const content = self.file.getContent();
        if (content.len == 0) {
            self.file.extendFile(3) catch {
                return context.ContextError.ExtendFileException;
            };
            self.pageflushed = 2;
            self.nfreelist = 0;
            self.nappend = 0;
            self.root = 0;

            var data1 = [_]u8{0} ** node.BTREE_PAGE_SIZE;
            var newNode = node.BNode.initSigleCapacity(&data1);
            freelist.flnSetHeader(&newNode, 0, 0);
            freelist.flnSetTotal(&newNode, 0);

            var c1 = self.file.getContent();
            const ptr = newNode.getdata();
            std.mem.copyForwards(u8, c1[node.BTREE_PAGE_SIZE..], ptr[0..node.BTREE_PAGE_SIZE]);

            self.freepages.head = 1;
            try self.masterStore();
            self.file.syncFile() catch {
                return context.ContextError.DataSaveException;
            };
            return;
        }

        // verify the page
        if (std.mem.eql(u8, DB_SIG, content[0..16]) == false) {
            return context.ContextError.LoadDataException;
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

        var freelist_head: u64 = 0;
        pos = 32;
        freelist_head |= @as(u64, content[pos + 0]) << 56;
        freelist_head |= @as(u64, content[pos + 1]) << 48;
        freelist_head |= @as(u64, content[pos + 2]) << 40;
        freelist_head |= @as(u64, content[pos + 3]) << 32;
        freelist_head |= @as(u64, content[pos + 4]) << 24;
        freelist_head |= @as(u64, content[pos + 5]) << 16;
        freelist_head |= @as(u64, content[pos + 6]) << 8;
        freelist_head |= @as(u64, content[pos + 7]);

        self.freepages.head = freelist_head;

        //todo init freelist
        //todo save freelist
        var bad: bool = !(1 <= used and used <= (content.len / node.BTREE_PAGE_SIZE));
        bad = bad or !(0 <= root and root < used);
        if (bad == true) {
            return context.ContextError.LoadDataException;
        }

        self.root = root;
        self.pageflushed = used;
        self.nfreelist = 0;
        self.nappend = 0;
    }

    // extend the file to at least `npages`.
    fn extendFile(self: *WindowsFreeListContext, npages: i64) context.ContextError!void {
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
            return context.ContextError.ExtendFileException;
        };
    }

    pub fn close(self: *WindowsFreeListContext) context.ContextError!void {
        try self.masterStore();
        self.file.syncFile() catch {
            return context.ContextError.DataSaveException;
        };
    }

    // update the master page. it must be atomic.
    fn masterStore(self: *WindowsFreeListContext) context.ContextError!void {
        var data: [40]u8 = [_]u8{0} ** 40;
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

        pos = 32;
        value = self.freepages.head;
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

    pub fn save(self: *WindowsFreeListContext) context.ContextError!void {
        try self.writePages();
        try self.syncPages();
    }

    fn writePages(self: *WindowsFreeListContext) context.ContextError!void {
        var listFreeNode = std.ArrayList(u64).init(self.allocator);
        defer listFreeNode.deinit();

        var iterator = self.updates.iterator();
        while (iterator.next()) |entry| {
            if (entry.value_ptr.* == null) {
                const ptr: u64 = entry.key_ptr.*;
                listFreeNode.append(ptr) catch {
                    unreachable;
                };
            }
        }
        self.freepages.Update(self.nfreelist, listFreeNode.items);

        const nPages: i64 = @intCast(self.pageflushed + self.nappend);
        self.extendFile(nPages) catch {
            return context.ContextError.ExtendFileException;
        };

        var it = self.updates.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.* != null) {
                const ptrMapped = self.getMappedPtr(entry.key_ptr.*) catch unreachable;
                const ptrUpdateNode = entry.value_ptr.*.?.getdata();
                std.mem.copyForwards(u8, ptrMapped, ptrUpdateNode[0..node.BTREE_PAGE_SIZE]);
            }
        }
    }

    fn syncPages(self: *WindowsFreeListContext) context.ContextError!void {
        self.file.syncFile() catch {
            return context.ContextError.DataSaveException;
        };

        self.clearUpdatePages();
        self.updates.clearRetainingCapacity();
        self.pageflushed += self.nappend;
        self.nfreelist = 0;
        self.nappend = 0;

        try self.masterStore();
        self.file.syncFile() catch {
            return context.ContextError.DataSaveException;
        };
    }

    fn clearUpdatePages(self: *WindowsFreeListContext) void {
        var iterator = self.updates.iterator();
        while (iterator.next()) |entry| {
            if (entry.value_ptr.* != null) {
                const n = entry.value_ptr.*.?;
                const ptr = n.data.Single;
                self.allocator.destroy(n);
                self.allocator.free(ptr);
            }
        }
    }

    pub fn get(self: *WindowsFreeListContext, ptr: u64) context.ContextError!node.BNode {
        const value = self.updates.get(ptr);
        if (value) |v| {
            return v.?.*;
        } else {
            return self.getMapped(ptr);
        }
    }

    pub fn getMapped(self: *WindowsFreeListContext, ptr: u64) context.ContextError!node.BNode {
        if (ptr > self.pageflushed + self.nappend) {
            std.debug.panic("Get Page Exception!", .{});
        }
        const offset = ptr * node.BTREE_PAGE_SIZE;
        const content = self.file.getContent();

        return node.BNode.initSigleCapacity(content[offset .. offset + node.BTREE_PAGE_SIZE][0..node.BTREE_PAGE_SIZE]);
    }

    pub fn getMappedPtr(self: *WindowsFreeListContext, ptr: u64) context.ContextError![]u8 {
        if (ptr > self.pageflushed + self.nappend) {
            std.debug.panic("Get Page Exception!", .{});
        }
        const offset = ptr * node.BTREE_PAGE_SIZE;
        const content = self.file.getContent();
        return content[offset .. offset + node.BTREE_PAGE_SIZE];
    }

    pub fn del(self: *WindowsFreeListContext, ptr: u64) bool {
        self.updates.put(ptr, null) catch {
            return false;
        };
        return true;
    }

    pub fn new(self: *WindowsFreeListContext, bnode: *node.BNode) context.ContextError!u64 {
        var ptr: u64 = 0;
        if (self.nfreelist < self.freepages.Total()) {
            // reuse a deallocated page
            ptr = self.freepages.Get(self.nfreelist);
            self.nfreelist += 1;
        } else {
            ptr = self.pageflushed + self.nappend;
            self.nappend += 1;
        }

        const newNode = self.copyBNode(bnode);
        self.updates.put(ptr, newNode) catch {
            return context.ContextError.DuplicateKey;
        };
        return ptr;
    }

    pub fn use(self: *WindowsFreeListContext, ptr: u64, bnode: *node.BNode) void {
        const newNode = self.copyBNode(bnode);
        self.updates.put(ptr, newNode) catch {
            unreachable;
        };
    }

    pub fn append(self: *WindowsFreeListContext, bnode: *node.BNode) u64 {
        const newNode = self.copyBNode(bnode);

        const ptr = self.pageflushed + self.nappend;
        self.nappend += 1;

        self.updates.put(ptr, newNode) catch {
            unreachable;
        };

        return ptr;
    }

    fn copyBNode(self: *WindowsFreeListContext, bnode: *node.BNode) *node.BNode {
        const ptrNodeData = bnode.getdata();
        var ptrData = self.allocator.alloc(u8, node.BTREE_PAGE_SIZE) catch {
            unreachable;
        };
        const newnode = self.allocator.create(node.BNode) catch {
            unreachable;
        };
        newnode.* = node.BNode{ .data = @unionInit(node.BNodeData, "Single", ptrData[0..node.BTREE_PAGE_SIZE]) };
        @memcpy(ptrData[0..node.BTREE_PAGE_SIZE], ptrNodeData[0..node.BTREE_PAGE_SIZE]);

        return newnode;
    }
};
