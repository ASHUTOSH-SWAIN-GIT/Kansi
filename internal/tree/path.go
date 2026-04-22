package tree

import (
	"errors"
	"strings"
)

// Path validation errors returned by ValidatePath.
var (
	ErrInvalidPath = errors.New("invalid path")
	ErrEmptyPath   = errors.New("path is empty")
)

// ValidatePath checks whether the znode path is well-formed.
// Rules: must start with "/", no trailing "/" (except root),
// no empty segments ("//"), no null bytes.
func ValidatePath(path string) error {
	if path == "" {
		return ErrEmptyPath
	}
	if path[0] != '/' {
		return ErrInvalidPath
	}
	// root "/" is fine; anything else ending in "/" is not
	if len(path) > 1 && path[len(path)-1] == '/' {
		return ErrInvalidPath
	}
	if strings.Contains(path, "//") {
		return ErrInvalidPath
	}
	for _, r := range path {
		if r == 0 {
			return ErrInvalidPath
		}
	}
	return nil
}

// SplitPath breaks "/a/b/c" into ["a", "b", "c"].
// Root "/" returns an empty slice.
func SplitPath(path string) []string {
	if path == "/" {
		return []string{}
	}
	// leading "/" makes Split produce an empty first element; drop it
	return strings.Split(path, "/")[1:]
}

// ParentPath returns the parent of a path.
// Parent of "/a/b" is "/a"; parent of "/a" is "/"; parent of "/" is "".
func ParentPath(path string) string {
	if path == "/" {
		return ""
	}
	i := strings.LastIndex(path, "/")
	if i == 0 {
		return "/"
	}
	return path[:i]
}

// BaseName returns the final segment.
// BaseName of "/a/b/c" is "c"; BaseName of "/" is "".
func BaseName(path string) string {
	if path == "/" {
		return ""
	}
	i := strings.LastIndex(path, "/")
	return path[i+1:]
}
