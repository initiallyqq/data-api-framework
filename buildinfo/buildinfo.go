// Package buildinfo carries the version stamped at build time: the
// repository's `git describe` output -- a tag such as 0.1.0, or a short
// commit for a tree that has no semantic tag yet. "dev" means the binary was
// built without the stamp (go run, go test).
package buildinfo

// Version is the release the binary reports. The build stamps it with
// -ldflags "-X github.com/initiallyqq/data-api-framework/buildinfo.Version=…";
// the source carries no version of its own.
var Version = "dev"
