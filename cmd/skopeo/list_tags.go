package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	commonFlag "github.com/containers/common/pkg/flag"
	"github.com/containers/common/pkg/retry"
	"github.com/containers/image/v5/docker"
	"github.com/containers/image/v5/docker/archive"
	"github.com/containers/image/v5/docker/reference"
	"github.com/containers/image/v5/image"
	"github.com/containers/image/v5/transports/alltransports"
	"github.com/containers/image/v5/types"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/mod/semver"
)

// tagListOutput is the output format of (skopeo list-tags), primarily so that we can format it with a simple json.MarshalIndent.
type tagListOutput struct {
	Repository string `json:",omitempty"`
	Tags       []string
}

type filteredTags struct {
	ToPrune []string
	ToKeep  []string
	Invalid []string
}

func newFilteredTags() *filteredTags {
	return &filteredTags{
		ToPrune: make([]string, 0),
		ToKeep:  make([]string, 0),
		Invalid: make([]string, 0),
	}
}

type tagFilterOptions struct {
	BeforeVersion commonFlag.OptionalString
	VersionLabel  commonFlag.OptionalString
	Valid         commonFlag.OptionalBool
	Invalid       commonFlag.OptionalBool
	SingleStream  commonFlag.OptionalBool
}

func (opts *tagFilterOptions) FilterPresent() bool {
	return opts.BeforeVersion.Present() || opts.Valid.Present() || opts.Invalid.Present()
}

func filterFlags() (pflag.FlagSet, *tagFilterOptions) {
	opts := tagFilterOptions{}
	fs := pflag.FlagSet{}
	fs.Var(commonFlag.NewOptionalStringValue(&opts.BeforeVersion), "before-version", "A version threshold prior to which to list tags")
	fs.Var(commonFlag.NewOptionalStringValue(&opts.VersionLabel), "version-label", "A label from which to derive the version for each tag")
	commonFlag.OptionalBoolFlag(&fs, &opts.Valid, "valid", "Whether to list only tags with valid semver")
	commonFlag.OptionalBoolFlag(&fs, &opts.Invalid, "invalid", "Whether to list only tags with invalid semver")
	commonFlag.OptionalBoolFlag(&fs, &opts.SingleStream, "single-stream", "Whether to only list versions matching the Major.Minor of the --before-version threshold")
	return fs, &opts
}

type tagsOptions struct {
	global     *globalOptions
	image      *imageOptions
	retryOpts  *retry.Options
	filterOpts *tagFilterOptions
}

var transportHandlers = map[string]func(ctx context.Context, sys *types.SystemContext, opts *tagsOptions, userInput string) (repositoryName string, tagListing []string, err error){
	docker.Transport.Name():  listDockerRepoTags,
	archive.Transport.Name(): listDockerArchiveTags,
}

// supportedTransports returns all the supported transports
func supportedTransports(joinStr string) string {
	res := slices.Sorted(maps.Keys(transportHandlers))
	return strings.Join(res, joinStr)
}

func tagsCmd(global *globalOptions) *cobra.Command {
	sharedFlags, sharedOpts := sharedImageFlags()
	imageFlags, imageOpts := dockerImageFlags(global, sharedOpts, nil, "", "")
	retryFlags, retryOpts := retryFlags()
	filterFlags, filterOpts := filterFlags()

	opts := tagsOptions{
		global:    global,
		image:     imageOpts,
		retryOpts: retryOpts,
		filterOpts: filterOpts,
	}

	cmd := &cobra.Command{
		Use:   "list-tags [command options] SOURCE-IMAGE",
		Short: "List tags in the transport/repository specified by the SOURCE-IMAGE",
		Long: `Return the list of tags from the transport/repository "SOURCE-IMAGE"

Supported transports:
` + supportedTransports(" ") + `

See skopeo-list-tags(1) section "REPOSITORY NAMES" for the expected format
`,
		RunE:    commandAction(opts.run),
		Example: `skopeo list-tags docker://docker.io/fedora`,
	}
	adjustUsage(cmd)
	flags := cmd.Flags()
	flags.AddFlagSet(&sharedFlags)
	flags.AddFlagSet(&imageFlags)
	flags.AddFlagSet(&retryFlags)
	flags.AddFlagSet(&filterFlags)
	return cmd
}

// Customized version of the alltransports.ParseImageName and docker.ParseReference that does not place a default tag in the reference
// Would really love to not have this, but needed to enforce tag-less and digest-less names
func parseDockerRepositoryReference(refString string) (types.ImageReference, error) {
	dockerRefString, ok := strings.CutPrefix(refString, docker.Transport.Name()+"://")
	if !ok {
		return nil, fmt.Errorf("docker: image reference %s does not start with %s://", refString, docker.Transport.Name())
	}

	ref, err := reference.ParseNormalizedNamed(dockerRefString)
	if err != nil {
		return nil, err
	}

	if !reference.IsNameOnly(ref) {
		return nil, errors.New(`No tag or digest allowed in reference`)
	}

	// Checks ok, now return a reference. This is a hack because the tag listing code expects a full image reference even though the tag is ignored
	return docker.NewReference(reference.TagNameOnly(ref))
}

func getValidSemverString(version string) (string, error) {
	// Return the version string if it's valid on its own
	if semver.IsValid(version) {
		return version, nil
	}

	// Append a "v" prefix onto the version string if it's missing one
	modifiedVersion := fmt.Sprintf("v%s", version)
	if semver.IsValid(modifiedVersion) {
		return modifiedVersion, nil
	}

	// If neither are valid, then the version string itself is bad
	return "", fmt.Errorf("invalid semver: %s", version)
}

func filterDockerTagBySemver(filtered *filteredTags, opts *tagsOptions, thresholdVersion string, tag string, tagVersion string) (error) {
	// Parse the threshold & tag versions
	validThreshold, err := getValidSemverString(thresholdVersion)
	if err != nil {
		return fmt.Errorf("invalid semver in version threshold: %w", err)
	}
	validTagVersion, err := getValidSemverString(tagVersion)

	// If the tag version is invalid, then filter it as such
	if err != nil {
		filtered.Invalid = append(filtered.Invalid, tag)
		return nil
	}

	// If single stream, keep all tags not matching threshold Major.Minor
	if opts.filterOpts.SingleStream.Present() && opts.filterOpts.SingleStream.Value() {
		if semver.MajorMinor(validThreshold) != semver.MajorMinor(validTagVersion) {
			filtered.ToKeep = append(filtered.ToKeep, tag)
			return nil
		}
	}

	// Compare the tag semver against the threshold semver
	cmp := semver.Compare(validThreshold, validTagVersion)
	if cmp < 0 {
		filtered.ToKeep = append(filtered.ToKeep, tag)
	} else {
		filtered.ToPrune = append(filtered.ToPrune, tag)
	}
	return nil
}

func filterDockerTagsByTagSemver(opts *tagsOptions, tags *tagListOutput) (*filteredTags, error) {
	// Get the user-provided threshold version
	// This will be validated later when the comparison takes place
	var threshold string
	if opts.filterOpts.BeforeVersion.Present() {
		threshold = opts.filterOpts.BeforeVersion.Value()
	} else {
		// Set as an arbitrary valid version since this isn't going to affect output
		threshold = "v0.1.0"
	}

	// Loop through each tag and sort into to prune, to keep, and, invalid
	filtered := newFilteredTags()
	for _, tag := range tags.Tags {
		err := filterDockerTagBySemver(filtered, opts, threshold, tag, tag)
		if err != nil {
			return nil, fmt.Errorf("Error filtering tags: %w", err)
		}
	}
	return filtered, nil
}

func filterDockerTagsByLabelSemver(ctx context.Context, sys *types.SystemContext, opts *tagsOptions, tags *tagListOutput) (filtered *filteredTags, retErr error) {
	// Get the user-provided threshold version
	// This will be validated later when the comparison takes place
	var threshold string
	if opts.filterOpts.BeforeVersion.Present() {
		threshold = opts.filterOpts.BeforeVersion.Value()
	} else {
		// Set as an arbitrary valid version since this isn't going to affect output
		threshold = "v0.1.0"
	}

	// Loop through each tag and inspect for its labels
	filtered = newFilteredTags()
	for _, tag := range tags.Tags {
		var (
			src         types.ImageSource
			imgInspect  *types.ImageInspectInfo
			err error
		)

		// Reconstruct the image reference and inspect it, borrowed from inspect implementation
		// Hardcode to docker:// as we know only this transport will trigger this function
		imageName := fmt.Sprintf("docker://%s:%s", tags.Repository, tag)
		if err := retry.IfNecessary(ctx, func() error {
			src, err = parseImageSource(ctx, opts.image, imageName)
			return err
		}, opts.retryOpts); err != nil {
			return nil, fmt.Errorf("Error parsing image name %q: %w", imageName, err)
		}
		defer func() {
			if err := src.Close(); err != nil {
				retErr = noteCloseFailure(retErr, "closing image", err)
			}
		}()
		unparsedInstance := image.UnparsedInstance(src, nil)
		img, err := image.FromUnparsedImage(ctx, sys, unparsedInstance)
		if err != nil {
			return nil, fmt.Errorf("Error parsing manifest for image: %w", err)
		}
		if err := retry.IfNecessary(ctx, func() error {
			imgInspect, err = img.Inspect(ctx)
			return err
		}, opts.retryOpts); err != nil {
			return nil, err
		}

		// Get the version label and compare
		versionLabel := opts.filterOpts.VersionLabel.Value()
		tagVersion, ok := imgInspect.Labels[versionLabel]
		if !ok {
			return nil, fmt.Errorf("Version label not found: %s", versionLabel)
		}
		err = filterDockerTagBySemver(filtered, opts, threshold, tag, tagVersion)
		if err != nil {
			return nil, fmt.Errorf("Error filtering tags: %w", err)
		}
	}
	return filtered, nil
}

func filterDockerTags(ctx context.Context, sys *types.SystemContext, opts *tagsOptions, repositoryName string, tags []string) (string, []string, error) {
	tagList := &tagListOutput{
		Repository: repositoryName,
		Tags:       tags,
	}
	var filtered *filteredTags
	var err error
	if opts.filterOpts.VersionLabel.Present() {
		filtered, err = filterDockerTagsByLabelSemver(ctx, sys, opts, tagList)
	} else {
		filtered, err = filterDockerTagsByTagSemver(opts, tagList)
	}
	if err != nil {
		return ``, nil, fmt.Errorf("Error filtering tags by semver: %w", err)
	}
	if opts.filterOpts.BeforeVersion.Present() {
		// Optionally only list tags prior to given semver threshold
		return repositoryName, filtered.ToPrune, nil
	} else if opts.filterOpts.Invalid.Present() {
		// Optionally only list tags with invalid semver
		return repositoryName, filtered.Invalid, nil
	} else { // Then only --valid could have possibly been set
		// Optionally only list tags with valid semver
		valid := append(filtered.ToKeep, filtered.ToPrune...)
		return repositoryName, valid, nil
	}
}

// List the tags from a repository contained in the imgRef reference. Any tag value in the reference is ignored
func listDockerTags(ctx context.Context, sys *types.SystemContext, opts *tagsOptions, imgRef types.ImageReference) (string, []string, error) {
	repositoryName := imgRef.DockerReference().Name()

	tags, err := docker.GetRepositoryTags(ctx, sys, imgRef)
	if err != nil {
		return ``, nil, fmt.Errorf("Error listing repository tags: %w", err)
	}

	// If the user requests all tags before a certain threshold, then filter
	if opts.filterOpts.FilterPresent() {
		return filterDockerTags(ctx, sys, opts, repositoryName, tags)
	}

	return repositoryName, tags, nil
}

// return the tagLists from a docker repo
func listDockerRepoTags(ctx context.Context, sys *types.SystemContext, opts *tagsOptions, userInput string) (repositoryName string, tagListing []string, err error) {
	// Do transport-specific parsing and validation to get an image reference
	imgRef, err := parseDockerRepositoryReference(userInput)
	if err != nil {
		return
	}
	if err = retry.IfNecessary(ctx, func() error {
		repositoryName, tagListing, err = listDockerTags(ctx, sys, opts, imgRef)
		return err
	}, opts.retryOpts); err != nil {
		return
	}
	return
}

// return the tagLists from a docker archive file
func listDockerArchiveTags(_ context.Context, sys *types.SystemContext, _ *tagsOptions, userInput string) (repositoryName string, tagListing []string, err error) {
	ref, err := alltransports.ParseImageName(userInput)
	if err != nil {
		return
	}

	tarReader, _, err := archive.NewReaderForReference(sys, ref)
	if err != nil {
		return
	}
	defer tarReader.Close()

	imageRefs, err := tarReader.List()
	if err != nil {
		return
	}

	var repoTags []string
	for imageIndex, items := range imageRefs {
		for _, ref := range items {
			repoTags, err = tarReader.ManifestTagsForReference(ref)
			if err != nil {
				return
			}
			// handle for each untagged image
			if len(repoTags) == 0 {
				repoTags = []string{fmt.Sprintf("@%d", imageIndex)}
			}
			tagListing = append(tagListing, repoTags...)
		}
	}

	return
}

func (opts *tagsOptions) run(args []string, stdout io.Writer) (retErr error) {
	ctx, cancel := opts.global.commandTimeoutContext()
	defer cancel()

	if len(args) != 1 {
		return errorShouldDisplayUsage{errors.New("Exactly one non-option argument expected")}
	}

	sys, err := opts.image.newSystemContext()
	if err != nil {
		return err
	}

	transport := alltransports.TransportFromImageName(args[0])
	if transport == nil {
		return fmt.Errorf("Invalid %q: does not specify a transport", args[0])
	}

	var repositoryName string
	var tagListing []string

	if val, ok := transportHandlers[transport.Name()]; ok {
		repositoryName, tagListing, err = val(ctx, sys, opts, args[0])
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("Unsupported transport '%s' for tag listing. Only supported: %s",
			transport.Name(), supportedTransports(", "))
	}

	outputData := tagListOutput{
		Repository: repositoryName,
		Tags:       tagListing,
	}

	out, err := json.MarshalIndent(outputData, "", "    ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "%s\n", string(out))

	return err
}
