package utils

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
)

func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func AvailableDiskSize() (uint64, error) {

	dir, _ := os.Getwd()
	// 加载 kernel32.dll
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	// 获取 GetDiskFreeSpaceExW 函数
	procGetDiskFreeSpaceExW := kernel32.NewProc("GetDiskFreeSpaceExW")

	// 将路径转换为 UTF-16
	pathPtr, err := syscall.UTF16PtrFromString(dir)
	if err != nil {
		return 0, err
	}

	var freeBytesAvailable, _, _ int64

	// 调用 GetDiskFreeSpaceExW
	ret, _, err := procGetDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		0,
		0,
	)

	if ret == 0 {
		return 0, err
	}

	return uint64(freeBytesAvailable), nil
}

func CopyDir(src, dest string, exclude []string) error {
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			return nil
		}
		for _, e := range exclude {
			matched, err := filepath.Match(e, fileName)
			if err == nil {
				return err
			}
			if matched {
				return nil
			}
		}

		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}

		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())
	})
}
