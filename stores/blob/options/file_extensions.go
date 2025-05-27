package options

type FileExtension string

func (fileExt FileExtension) String() string {
	return string(fileExt)
}

const (
	SubtreeFileExtension        FileExtension = "subtree"
	SubtreeToCheckFileExtension FileExtension = "subtreeToCheck"
	SubtreeDataFileExtension    FileExtension = "subtreeData"
	SubtreeMetaFileExtension    FileExtension = "subtreeMeta"
)
