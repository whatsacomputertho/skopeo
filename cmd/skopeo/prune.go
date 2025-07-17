package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/containers/common/pkg/retry"
	"github.com/containers/image/v5/manifest"
	"github.com/containers/image/v5/docker"
	"github.com/containers/image/v5/transports/alltransports"
	"github.com/containers/image/v5/types"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Enumerated constant for byte units
const (
	_  = iota             // Ignore first value
	KB = 1 << (10 * iota) // Bit shift each by 10
	MB
	GB
	TB
	PB
	EB
)

// Global map of byte unit names to byte unit sizes
var byteUnits = map[int]string{
	KB: "KB",
	MB: "MB",
	GB: "GB",
	TB: "TB",
	PB: "PB",
	EB: "EB",
}

// Formats a number of bytes to the nearest unit
// For example, 2048 -> "2.00 KB"
func bytesToByteUnit(bytes int64) string {
	// Initialize variable to track unit decimal
	var unitDec float64
	unitSuf := "B"

	// Collect and sort the byte unit sizes
	sizes := make([]int, 0)
	for k, _ := range byteUnits {
		sizes = append(sizes, k)
	}
	sort.Ints(sizes)

	// Loop through byte units
	for _, size := range sizes {
		// Divide by the unit size
		tempDec := float64(bytes) / float64(size)

		// If less than 1 then break
		if tempDec < 1 {
			break
		}

		// Otherwise overwrite the unit suffix
		unitDec = tempDec
		unitSuf = byteUnits[size]
	}

	// Format and return
	return fmt.Sprintf("%.2f %s", unitDec, unitSuf)
}

var pruneTransportHandlers = map[string]func(ctx context.Context, sys *types.SystemContext, opts *pruneOptions, userInput string) error {
	docker.Transport.Name():  pruneDockerTags,
}

// supportedPrneTransports returns all the supported transports for pruning
func supportedPruneTransports(joinStr string) string {
	res := slices.Sorted(maps.Keys(pruneTransportHandlers))
	return strings.Join(res, joinStr)
}

type pruneUserOptions struct {
	SkipSummary bool
	NonInteractive bool
}

func pruneFlags() (pflag.FlagSet, *pruneUserOptions) {
	opts := pruneUserOptions{}
	fs := pflag.FlagSet{}
	fs.BoolVarP(&opts.SkipSummary, "skip-summary", "s", false, "Skip computing the prune summary of freed storage space")
	fs.BoolVarP(&opts.NonInteractive, "non-interactive", "y", false, "Do not display an interactive prompt for the user to confirm before beginning puning")
	return fs, &opts
}

type pruneOptions struct {
	global     *globalOptions
	image      *imageOptions
	retryOpts  *retry.Options
	filterOpts *tagFilterOptions
	pruneOpts *pruneUserOptions
}

func (p *pruneOptions) intoTagsOptions() *tagsOptions {
	return &tagsOptions{
		global: p.global,
		image: p.image,
		retryOpts: p.retryOpts,
		filterOpts: p.filterOpts,
	}
}

// Prune command
func pruneCmd(global *globalOptions) *cobra.Command {
	sharedFlags, sharedOpts := sharedImageFlags()
	imageFlags, imageOpts := dockerImageFlags(global, sharedOpts, nil, "", "")
	retryFlags, retryOpts := retryFlags()
	filterFlags, filterOpts := filterFlags()
	pruneFlags, pruneOpts := pruneFlags()

	opts := pruneOptions{
		global:    global,
		image:     imageOpts,
		retryOpts: retryOpts,
		filterOpts: filterOpts,
		pruneOpts: pruneOpts,
	}

	cmd := &cobra.Command{
		Use:   "prune [command options] SOURCE-IMAGE",
		Short: "Prune tags in the transport/repository specified by the SOURCE-IMAGE",
		Long: `Prune the list of tags from the transport/repository "SOURCE-IMAGE"

Supported transports:
` + supportedPruneTransports(" ") + `

See skopeo-prune(1) section "REPOSITORY NAMES" for the expected format
`,
		RunE:    commandAction(opts.run),
		Example: `skopeo prune docker://docker.io/fedora`,
	}
	adjustUsage(cmd)
	flags := cmd.Flags()
	flags.AddFlagSet(&sharedFlags)
	flags.AddFlagSet(&imageFlags)
	flags.AddFlagSet(&retryFlags)
	flags.AddFlagSet(&filterFlags)
	flags.AddFlagSet(&pruneFlags)
	return cmd
}

// Function that polls for user input confirming to continue with the pruning
func displayPrunePrompt() bool {
	// Prompt the user whether they would like to proceed to prune
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("warning: continuing to prune will lead to irreversible data loss")
		fmt.Print("continue to prune? (y/n): ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "y" || input == "Y" {
			fmt.Printf("continuing\n\n")
			break
		} else if input == "n" || input == "N" {
			fmt.Println("done")
			return false
		} else {
			fmt.Println("invalid input. please enter 'y' or 'n'")
		}
	}
	return true
}

func getLayerSizeMap(layerInfo []manifest.LayerInfo) map[string]int64 {
	// Initialize a layer size map, maps layer digest to size
	layerSizeMap := make(map[string]int64)

	// Loop through the layerInfo slice
	for _, layer := range layerInfo {
		// Get layer digest and size
		digest := string(layer.Digest)
		size := layer.Size

		// Continue if layer is unknown
		if digest == "" && size == -1 {
			continue
		}

		// Add to map
		layerSizeMap[digest] = size
	}

	// Return the map
	return layerSizeMap
}

func getManifestSizeMaps(rawManifest []byte, mimeType string) (map[string]int64, map[string]int64, error) {
	// Convert the raw manifest to a manifest
	mfs, err := manifest.FromBlob(rawManifest, mimeType)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert raw manifest to manifest: %w", err)
	}

	// Initialize size & size map vars
	var configSize int64
	configSizeMap := make(map[string]int64)
	layerSizeMap := make(map[string]int64)

	// Get the config size
	configInfo := mfs.ConfigInfo()
	if string(configInfo.Digest) != "" {
		configSize = configInfo.Size
	}

	// Initialzie the config size map, maps config digest to size
	configSizeMap[string(configInfo.Digest)] = configSize

	// Get layer sizes
	layerSizeMap = getLayerSizeMap(mfs.LayerInfos())

	return configSizeMap, layerSizeMap, nil
}

func getManifestListSizeMaps(ctx context.Context, sys *types.SystemContext, repo string, rawManifest []byte, mimeType string) (map[string]int64, map[string]int64, error) {
	// Convert the raw manifest to a slice of v2 descriptors
	manifestList, err := manifest.ListFromBlob(rawManifest, mimeType)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert raw manifest to manifest list: %w", err)
	}

	// Initialize maps for the config & layer sizes of each manifest
	configSizeMap := make(map[string]int64)
	layerSizeMap := make(map[string]int64)

	// Loop through the manifest list and append the size maps for each
	for _, digest := range manifestList.Instances() {
		// Format the image URL
		url := fmt.Sprintf("docker://%s@%s", repo, string(digest))

		// Parse the image URL into an image reference
		ref, err := alltransports.ParseImageName(url)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse image url: %s", err)
		}

		// Instantiate an image source from the reference
		src, err := ref.NewImageSource(ctx, sys)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to instantiate image source: %w", err)
		}
		defer src.Close()

		// Get the raw manifest
		rawManifest, _, err := src.GetManifest(ctx, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get raw manifest: %w", err)
		}
		mimeType := manifest.GuessMIMEType(rawManifest)

		// Get the size maps for the manifest
		tmpConfigSizeMap, tmpLayerSizeMap, err := getManifestSizeMaps(rawManifest, mimeType)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get size maps for raw manifest: %w", err)
		}

		// Add the size data to the list size maps
		for k, v := range tmpConfigSizeMap {
			configSizeMap[k] = v
		}
		for k, v := range tmpLayerSizeMap {
			layerSizeMap[k] = v
		}
	}
	return configSizeMap, layerSizeMap, nil
}

func getSizeMaps(ctx context.Context, sys *types.SystemContext, url string) (map[string]int64, map[string]int64, error) {
	// Parse the image URL into an image reference
	ref, err := alltransports.ParseImageName(url)
	if err != nil {
		return nil, nil, fmt.Errorf("Error parsing image reference: %w", err)
	}

	// Capture the repository of the image URL
	repo := ref.DockerReference().Name()

	// Instantiate an image source from the reference
	src, err := ref.NewImageSource(ctx, sys)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to instantiate image source: %w", err)
	}
	defer src.Close()

	// Get the raw manifest
	rawManifest, _, err := src.GetManifest(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get raw manifest: %w", err)
	}

	// Get the manifest MIME type
	var configSizeMap map[string]int64
	var layerSizeMap map[string]int64
	mimeType := manifest.GuessMIMEType(rawManifest)

	// Get the size maps based on whether the image is multi-arch or single-arch
	if manifest.MIMETypeIsMultiImage(mimeType) {
		// If multi-arch, then get the multi-arch size maps
		configSizeMap, layerSizeMap, err = getManifestListSizeMaps(ctx, sys, repo, rawManifest, mimeType)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get manifest list size maps: %w", err)
		}
	} else {
		// Otherwise get the single-arch size maps
		configSizeMap, layerSizeMap, err = getManifestSizeMaps(rawManifest, mimeType)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get manifest size maps: %w", err)
		}
	}

	// Return the size maps
	return configSizeMap, layerSizeMap, nil
}

// Function that computes the deduplicated size of a slice of image tags
func getDeduplicatedSizeParallel(ctx context.Context, sys *types.SystemContext, repositoryName string, tags []string) int64 {
	// Get the config & layer size maps
	dedupConfigSizeMap := make(map[string]int64)
	dedupLayerSizeMap := make(map[string]int64)

	// Channels for communication as sizes are calculated
	var sizeMapArrChan = make(chan [2]map[string]int64, 8)
	var errChan = make(chan error, 8)
	var doneChan = make(chan struct{})

	// Channel for limiting concurrent fetches from the registry
	var fetchLimitCh = make(chan struct{}, 8)

	// Total tags and counter for sizes calculated
	totalTags := len(tags)
	sizesCalculated := 0

	// Total size int
	var totalSize int64

	// Goroutine for monitoring progress
	var readWaitGroup = sync.WaitGroup{}
	readWaitGroup.Add(1)
	go func(sizeMapArrChan <-chan [2]map[string]int64, errChan <-chan error, doneChan <-chan struct{}) {
		// Loop for monitoring progress
	sizeCalcLoop:
		for {
			select {
			case sizeMapArr := <-sizeMapArrChan:
				// Another case of erroneous firing of the channel, not entirely sure why
				// Validating channel output seems to filter out erroneous instances
				if len(sizeMapArr[0]) > 0 && len(sizeMapArr[1]) > 0 {
					// Log progress in-place
					sizesCalculated += 1
					fmt.Printf("\rcalculating deduplicated image sizes\t%d / %d", sizesCalculated, totalTags)

					// Add to the deduplicated maps
					for configDigest, configSize := range sizeMapArr[0] {
						dedupConfigSizeMap[configDigest] = configSize
					}
					for layerDigest, layerSize := range sizeMapArr[1] {
						dedupLayerSizeMap[layerDigest] = layerSize
					}
				}
			case err := <-errChan:
				if err != nil {
					// Print the error
					fmt.Fprintf(os.Stderr, "\nerror while calculating deduplicated image sizes: %s\n", err)
				}
			case <-doneChan:
				break sizeCalcLoop
			}
		}

		// Log completion in-place
		fmt.Printf("\rcalculating deduplicated image sizes\t%d / %d (done)\n", sizesCalculated, totalTags)

		// Sum the deduplicated config and layer sizes and return
		for _, configSize := range dedupConfigSizeMap {
			totalSize += configSize
		}
		for _, layerSize := range dedupLayerSizeMap {
			totalSize += layerSize
		}

		// Close the goroutine
		readWaitGroup.Done()
	}(sizeMapArrChan, errChan, doneChan)

	// Goroutines for fetching image sizes
	var fetchWaitGroup = sync.WaitGroup{}
	for i := 0; i < len(tags); i++ {
		fetchWaitGroup.Add(1)
		fetchLimitCh <- struct{}{}
		url := fmt.Sprintf("docker://%s:%s", repositoryName, tags[i])
		go func(sys *types.SystemContext, url string, sizeMapArrChan chan<- [2]map[string]int64, errChan chan<- error, fetchLimitCh <-chan struct{}) {
			// Close the channel once complete
			defer func() { fetchWaitGroup.Done(); <-fetchLimitCh }()

			// Get the image size maps
			configSizeMap, layerSizeMap, err := getSizeMaps(ctx, sys, url)
			if err != nil {
				errChan <- err
				return
			}

			// Initialize a size-two array containing the size maps
			sizeMapArr := [2]map[string]int64{configSizeMap, layerSizeMap}
			sizeMapArrChan <- sizeMapArr
		}(sys, url, sizeMapArrChan, errChan, fetchLimitCh)
	}

	// Goroutines for monitoring fetch & read wait groups
	var monitorWaitGroup = sync.WaitGroup{}
	monitorWaitGroup.Add(2)
	go func(doneChan chan struct{}) {
		readWaitGroup.Wait()
		close(doneChan)
		monitorWaitGroup.Done()
	}(doneChan)
	go func(sizeMapArrChan chan [2]map[string]int64, errChan chan error, fetchLimitCh chan struct{}, doneChan chan struct{}) {
		fetchWaitGroup.Wait()
		close(sizeMapArrChan)
		close(errChan)
		close(fetchLimitCh)
		doneChan <- struct{}{} // Signal completion to the readWaitGroup
		monitorWaitGroup.Done()
	}(sizeMapArrChan, errChan, fetchLimitCh, doneChan)
	monitorWaitGroup.Wait()

	// Return the calculated total size
	return totalSize
}

// Function that displays the prune summary of size freed in pruning
// TODO: Determine if errors occurred during size calculation
func displayPruneSummary(ctx context.Context, sys *types.SystemContext, userInput string, toPrune []string, toKeep []string) error {
	imgRef, err := parseDockerRepositoryReference(userInput)
	if err != nil {
		return fmt.Errorf("Error parsing image reference: %w", err)
	}
	repositoryName := imgRef.DockerReference().Name()
	
	// Compute the deduplicated size of the images that will be pruned vs kept
	pruneSize := getDeduplicatedSizeParallel(ctx, sys, repositoryName, toPrune)
	keepSize := getDeduplicatedSizeParallel(ctx, sys, repositoryName, toKeep)

	// Summarize the list
	fmt.Println("")
	w := tabwriter.NewWriter(os.Stdout, 1, 1, 3, ' ', 0)
	fmt.Fprintln(w, "ACTION\tTAGS\tSIZE")
	pruneDisp := fmt.Sprintf("Prune\t%d\t%s", len(toPrune), bytesToByteUnit(pruneSize))
	keepDisp := fmt.Sprintf("Keep\t%d\t%s", len(toKeep), bytesToByteUnit(keepSize))
	fmt.Fprintln(w, pruneDisp)
	fmt.Fprintln(w, keepDisp)
	fmt.Fprintln(w, "")
	w.Flush()
	return nil // Success
}

// Function that prunes the tags identified for pruning and displays progress
func pruneDockerTagsParallel(ctx context.Context, sys *types.SystemContext, repositoryName string, tags []string) error {
	// Variable tracking how many tags have been pruned
	totalTags := len(tags)
	tagsPruned := 0

	// Channels for communication as tags are pruned
	var pruneCh = make(chan int, 8)
	var errCh = make(chan error, 8)
	var doneCh = make(chan struct{})

	// Channel for limiting concurrent prunes from the registry
	var pruneLimitCh = make(chan struct{}, 8)

	// Goroutine for monitoring progress
	var readWaitGroup = sync.WaitGroup{}
	readWaitGroup.Add(1)
	go func(pruneCh <-chan int, errCh <-chan error, doneCh <-chan struct{}) {
		// Close the read wait group once complete
		defer readWaitGroup.Done()

		// Loop for monitoring progress
	pruneProgressLoop:
		for {
			select {
			case sig := <-pruneCh:
				if sig != 0 {
					// Log progress in-place
					tagsPruned += 1
					fmt.Printf("\rpruning tags\t%d / %d", tagsPruned, totalTags)
				}
			case err := <-errCh:
				if err != nil {
					// Print the error
					fmt.Fprintf(os.Stderr, "\nerror while pruning tags %s\n", err)
				}
			case <-doneCh:
				break pruneProgressLoop
			}
		}

		// Log completion in-place
		fmt.Printf("\rpruning tags\t%d / %d (done)\n", tagsPruned, totalTags)
	}(pruneCh, errCh, doneCh)

	// Goroutines for pruning tags
	var pruneWaitGroup = sync.WaitGroup{}
	for i := 0; i < len(tags); i++ {
		pruneWaitGroup.Add(1)
		pruneLimitCh <- struct{}{}
		url := fmt.Sprintf("docker://%s:%s", repositoryName, tags[i])
		go func(sys *types.SystemContext, url string, pruneCh chan<- int, errCh chan<- error, pruneLimitCh <-chan struct{}) {
			// Close the prune wait group once complete
			defer func() { pruneWaitGroup.Done(); <-pruneLimitCh }()

			// Append docker transport to image url
			// This assumes all images are in remote distribution registry
			url = fmt.Sprintf("docker://%s", url)

			// Parse the image URL into a reference
			ref, err := alltransports.ParseImageName(url)
			if err != nil {
				errCh <- fmt.Errorf("failed to parse image url: %w", err)
				return
			}

			// Delete the image corresponding to the reference
			err = ref.DeleteImage(ctx, sys)
			if err != nil {
				errCh <- fmt.Errorf("failed to prune tag: %w", err)
				return
			}

			// Signal prune completion
			pruneCh <- 1
		}(sys, url, pruneCh, errCh, pruneLimitCh)
	}

	// Goroutines for monitoring prune wait groups
	var monitorWaitGroup = sync.WaitGroup{}
	monitorWaitGroup.Add(2)
	go func(doneCh chan struct{}) {
		readWaitGroup.Wait()
		close(doneCh)
		monitorWaitGroup.Done()
	}(doneCh)
	go func(pruneCh chan int, errCh chan error, pruneLimitCh chan struct{}, doneCh chan struct{}) {
		pruneWaitGroup.Wait()
		close(pruneCh)
		close(errCh)
		close(pruneLimitCh)
		doneCh <- struct{}{} // Signal completion to the readWaitGroup
		monitorWaitGroup.Done()
	}(pruneCh, errCh, pruneLimitCh, doneCh)
	monitorWaitGroup.Wait()
	return nil
}

// Function that gets the filtered tags to prune
func getFilteredDockerTags(ctx context.Context, sys *types.SystemContext, opts *tagsOptions, userInput string) (*filteredTags, error) {
	// Get the repo tags
	imgRef, err := parseDockerRepositoryReference(userInput)
	if err != nil {
		return nil, fmt.Errorf("Error parsing image reference: %w", err)
	}
	repositoryName := imgRef.DockerReference().Name()
	tags, err := docker.GetRepositoryTags(ctx, sys, imgRef)
	if err != nil {
		return nil, fmt.Errorf("Error getting repository tags: %w", err)
	}

	// If the user requests all tags before a certain threshold, then filter
	if opts.filterOpts.FilterPresent() {
		return filterDockerTags(ctx, sys, opts, repositoryName, tags)
	}

	// Otherwise, keep all tags as the default behavior to avoid data loss
	filtered := &filteredTags{
		ToPrune: make([]string, 0),
		ToKeep: tags,
		Invalid: make([]string, 0),
	}
	return filtered, nil
}

// Function that prunes docker tags
func pruneDockerTags(ctx context.Context, sys *types.SystemContext, opts *pruneOptions, userInput string) error {
	// Get the filtered docker tags for the given repository
	filteredTags, err := getFilteredDockerTags(ctx, sys, opts.intoTagsOptions(), userInput)
	if err != nil {
		return fmt.Errorf("Error getting filtered docker tags: %w", err)
	}

	// Determine which images to prune vs keep based on user input
	var toPrune []string
	var toKeep []string
	if opts.filterOpts.Invalid.Present() {
		toPrune = append(filteredTags.ToPrune, filteredTags.Invalid...)
		toKeep = filteredTags.ToKeep
	} else {
		toPrune = filteredTags.ToPrune
		toKeep = append(filteredTags.ToKeep, filteredTags.Invalid...)
	}

	// Display the prune summary
	// TODO: Error check - determine if errors occurred during size calculation
	if !opts.pruneOpts.SkipSummary {
		err = displayPruneSummary(ctx, sys, userInput, toPrune, toKeep)
		if err != nil {
			return fmt.Errorf("Error displaying prune summary: %w", err)
		}
	}

	// Display the prune prompt
	if !opts.pruneOpts.NonInteractive {
		accepted := displayPrunePrompt()
		if !accepted{
			return nil // User decided not to prune
		}
	}

	// Prune the docker tags
	err = pruneDockerTagsParallel(ctx, sys, userInput, toPrune)
	if err != nil {
		return fmt.Errorf("Error pruning tags: %w", err)
	}
	return nil // Success
}

func (opts *pruneOptions) run(args []string, stdout io.Writer) (retErr error) {
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

	if val, ok := pruneTransportHandlers[transport.Name()]; ok {
		err = val(ctx, sys, opts, args[0])
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("Unsupported transport '%s' for tag listing. Only supported: %s",
			transport.Name(), supportedPruneTransports(", "))
	}

	return nil // Success
}
