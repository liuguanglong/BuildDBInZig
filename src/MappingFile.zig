const std = @import("std");
const windows = @import("std").os.windows;
const ntdll = windows.ntdll;
const kernel32 = windows.kernel32;

pub extern "kernel32" fn CreateFileA(
    lpFileName: [*:0]const u8,
    dwDesiredAccess: windows.DWORD,
    dwShareMode: windows.DWORD,
    lpSecurityAttributes: ?*windows.SECURITY_ATTRIBUTES,
    dwCreationDisposition: windows.DWORD,
    dwFlagsAndAttributes: windows.DWORD,
    hTemplateFile: ?windows.HANDLE,
) callconv(windows.WINAPI) windows.HANDLE;

pub extern "kernel32" fn CreateFileMappingA(hFile: windows.HANDLE, lpFileMappingAttributes: ?*windows.SECURITY_ATTRIBUTES, flProtect: windows.DWORD, dwMaximumSizeHigh: windows.DWORD, dwMaximumSizeLow: windows.DWORD, lpName: [*:0]const u8) callconv(windows.WINAPI) windows.HANDLE;
pub extern "kernel32" fn MapViewOfFile(hFileMappingObject: windows.HANDLE, dwDesiredAccess: windows.DWORD, dwFileOffsetHigh: windows.DWORD, dwFileOffsetLow: windows.DWORD, dwNumberOfBytesToMap: windows.DWORD) callconv(windows.WINAPI) ?windows.LPVOID;
pub extern "kernel32" fn UnmapViewOfFile(lpBaseAddress: ?windows.LPVOID) callconv(windows.WINAPI) windows.BOOL;
pub extern "kernel32" fn FlushViewOfFile(lpBaseAddress: ?windows.LPVOID, dwNumberOfBytesToFlush: windows.SIZE_T) callconv(windows.WINAPI) windows.BOOL;

pub extern "ntdll" fn NtExtendSection(
    SectionHandle: windows.HANDLE,
    NewSectionSize: ?*windows.LARGE_INTEGER,
) callconv(windows.WINAPI) windows.NTSTATUS;

pub extern "ntdll" fn NtMapViewOfSection(SectionHandle: windows.HANDLE, ProcessHandle: windows.HANDLE, BaseAddress: *?windows.PVOID, ZeroBits: ?*windows.ULONG, CommitSize: windows.SIZE_T, SectionOffset: ?*windows.LARGE_INTEGER, ViewSize: *windows.SIZE_T, InheritDisposition: windows.SECTION_INHERIT, AllocationType: windows.ULONG, Win32Protect: windows.ULONG) callconv(windows.WINAPI) windows.NTSTATUS;
pub extern "ntdll" fn NtUnmapViewOfSection(ProcessHandle: windows.HANDLE, BaseAddress: ?windows.LPVOID) callconv(windows.WINAPI) windows.NTSTATUS;

pub const FILE_MAP_WRITE = windows.SECTION_MAP_WRITE;
pub const FILE_MAP_READ = windows.SECTION_MAP_READ;
pub const FILE_MAP_ALL_ACCESS = windows.SECTION_ALL_ACCESS;
//pub const FILE_MAP_EXECUTE = windows.SECTION_MAP_EXECUTE_EXPLICIT; // not included in FILE_MAP_ALL_ACCESS

pub const FILE_MAP_COPY = 0x00000001;
pub const FILE_MAP_RESERVE = 0x80000000;
pub const FILE_MAP_TARGETS_INVALID = 0x40000000;
pub const FILE_MAP_LARGE_PAGES = 0x20000000;

pub const FileMappingError = error{ OpenFileException, MappingFileException, GetFileSizeException, NtCreateSectionException, NetMapViewOfSectionException, NtExtendSectionException, FlushViewofFileException, FlushFileBUffersException };

pub const MappingFile = struct {
    hFile: windows.HANDLE,
    hSection: windows.HANDLE,
    lpBaseAddress: ?windows.LPVOID,
    dwPageSize: u32,
    fileSize: i64,

    pub fn init(fileName: [*:0]const u8, pageSize: u32, maxPageCount: u64) !MappingFile {
        var stdout = std.io.getStdOut().writer();

        //Open File
        const hFile = CreateFileA(fileName, windows.GENERIC_READ | windows.GENERIC_WRITE, 0, null, windows.OPEN_ALWAYS, windows.FILE_ATTRIBUTE_NORMAL, null);
        if (hFile == windows.INVALID_HANDLE_VALUE) {
            std.debug.print("Failed to open file. Error: {d}\n", .{kernel32.GetLastError()});
            return FileMappingError.OpenFileException;
        }

        //get File Size
        var file_size: windows.LARGE_INTEGER = undefined;
        if (kernel32.GetFileSizeEx(hFile, &file_size) == 0) {
            std.debug.print("Failed to get file size. Error: {d}\n", .{kernel32.GetLastError()});
            return FileMappingError.GetFileSizeException;
        }
        std.debug.print("file size: {d}\n", .{file_size});

        var SectionSize: windows.LARGE_INTEGER = @intCast(pageSize * 1);
        var hSection: windows.HANDLE = undefined;
        const status = ntdll.NtCreateSection(&hSection, windows.SECTION_EXTEND_SIZE | windows.SECTION_MAP_READ | windows.SECTION_MAP_WRITE, null, &SectionSize, windows.PAGE_READWRITE, windows.SEC_COMMIT, hFile);
        if (status != windows.NTSTATUS.SUCCESS) {
            std.debug.print("Failed NtCreateSection. Error: {d}\n", .{status});
            return FileMappingError.NtCreateSectionException;
        }

        const ProcessHandle = windows.GetCurrentProcess();
        var ViewSize: usize = pageSize * maxPageCount;
        var lpZwMapping: ?*anyopaque = null;
        var ZeroBits: u32 = 0;
        const statusZwMapping = NtMapViewOfSection(hSection, ProcessHandle, &lpZwMapping, &ZeroBits, 0, null, &ViewSize, windows.SECTION_INHERIT.ViewUnmap, windows.MEM_RESERVE, windows.PAGE_READWRITE);
        if (statusZwMapping != windows.NTSTATUS.SUCCESS) {
            try stdout.print("Failed NtMapViewOfSection. Error: {d}\n", .{statusZwMapping});
            return FileMappingError.NetMapViewOfSectionException;
        }

        if (lpZwMapping != windows.INVALID_HANDLE_VALUE) {
            return MappingFile{ .hFile = hFile, .fileSize = file_size, .hSection = hSection, .lpBaseAddress = lpZwMapping, .dwPageSize = pageSize };
        } else {
            return FileMappingError.MappingFileException;
        }
    }

    pub fn getContent(self: MappingFile) []u8 {
        const size: usize = @intCast(self.fileSize);
        const mapped_ptr: [*]u8 = @ptrCast(self.lpBaseAddress);
        const content = mapped_ptr[0..size];

        return content;
    }

    pub fn deinit(self: MappingFile) void {
        const ProcessHandle = windows.GetCurrentProcess();
        _ = kernel32.CloseHandle(self.hFile);
        _ = kernel32.CloseHandle(self.hSection);
        _ = NtUnmapViewOfSection(ProcessHandle, self.lpBaseAddress);
    }

    pub fn extendFile(self: *MappingFile, pageCount: i64) !void {
        var SectionSize: windows.LARGE_INTEGER = self.fileSize + pageCount * self.dwPageSize;
        const statusExtend = NtExtendSection(self.hSection, &SectionSize);
        if (statusExtend != windows.NTSTATUS.SUCCESS) {
            std.debug.print("Failed ExtendSection. Error: {d}\n", .{statusExtend});
            return FileMappingError.NtExtendSectionException;
        }

        self.fileSize = SectionSize;
    }

    pub fn syncFile(self: *MappingFile) !void {
        if (FlushViewOfFile(self.lpBaseAddress, 0) == 0) {
            std.debug.print("Failed to flush view of file. Error: {}\n", .{kernel32.GetLastError()});
            return FileMappingError.FlushViewofFileException;
        }

        if (kernel32.FlushFileBuffers(self.hFile) == 0) {
            std.debug.print("Failed to flush file buffers. Error: {}\n", .{kernel32.GetLastError()});
            return FileMappingError.FlushFileBUffersException;
        }
    }
};
