/*
 *     Copyright 2022 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/dfstore"
)

var copyDescription = "copies a local file or dragonfly object to another location locally or in dragonfly object storage."

// copyCmd represents to copy object between object storage and local.
var copyCmd = &cobra.Command{
	Use:                "cp <source> <target> [flags]",
	Short:              copyDescription,
	Long:               copyDescription,
	Args:               cobra.ExactArgs(2),
	DisableAutoGenTag:  true,
	SilenceUsage:       true,
	FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := cfg.Validate(); err != nil {
			return err
		}

		if err := validateCopyArgs(args); err != nil {
			return err
		}

		source := args[0]
		target := args[1]

		// Copy object storage to local file.
		if isDfstoreURL(source) {
			bucketName, objectKey, err := parseDfstoreURL(source)
			if err != nil {
				return err
			}

			if err := copyObjectStorageToLocalFile(ctx, cfg, bucketName, objectKey, target); err != nil {
				return err
			}

			return nil
		}

		// Copy local file to object storage.
		bucketName, objectKey, err := parseDfstoreURL(target)
		if err != nil {
			return err
		}

		if err := copyLocalFileToObjectStorage(ctx, cfg, bucketName, objectKey, source); err != nil {
			return err
		}

		return nil
	},
}

func init() {
	// Bind more cache specific persistent flags.
	flags := copyCmd.Flags()
	flags.StringVar(&cfg.Filter, "filter", cfg.Filter, "filter is used to generate a unique task id by filtering unnecessary query params in the URL, it is separated by & character")
	flags.IntVarP(&cfg.Mode, "mode", "m", cfg.Mode, "mode is the mode in which the backend is written, when the value is 0, it represents AsyncWriteBack, and when the value is 1, it represents WriteBack")
	flags.IntVar(&cfg.MaxReplicas, "max-replicas", cfg.MaxReplicas, "maxReplicas is the maximum number of replicas of an object cache in seed peers")

	// Bind common flags.
	if err := viper.BindPFlags(flags); err != nil {
		panic(err)
	}
}

// Validate copy arguments.
func validateCopyArgs(args []string) error {
	if isDfstoreURL(args[0]) && isDfstoreURL(args[1]) {
		return errors.New("source and target url cannot both be dfs:// protocol")
	}

	if !isDfstoreURL(args[0]) && !isDfstoreURL(args[1]) {
		return errors.New("source and target url cannot both be local file path")
	}

	return nil
}

// Copy object storage to local file.
func copyObjectStorageToLocalFile(ctx context.Context, cfg *config.DfstoreConfig, bucketName, objectKey, filepath string) error {
	start := time.Now()
	dfs := dfstore.New(cfg.Endpoint)
	meta, err := dfs.GetObjectMetadataWithContext(ctx, &dfstore.GetObjectMetadataInput{
		BucketName: bucketName,
		ObjectKey:  objectKey,
	})
	if err != nil {
		return err
	}

	bar := progressbar.NewOptions64(
		meta.ContentLength,
		progressbar.OptionShowBytes(true),
		progressbar.OptionUseANSICodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetDescription("[cyan]Downloading...[reset]"),
		progressbar.OptionSetRenderBlankState(true),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	reader, err := dfs.GetObjectWithContext(ctx, &dfstore.GetObjectInput{
		BucketName: bucketName,
		ObjectKey:  objectKey,
	})
	if err != nil {
		return err
	}
	defer reader.Close()

	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(io.MultiWriter(f, bar), reader); err != nil {
		return err
	}

	fmt.Printf("download object storage success, length: %d bytes cost: %d ms", meta.ContentLength, time.Since(start).Milliseconds())
	return nil
}

// Copy local file to object storage.
func copyLocalFileToObjectStorage(ctx context.Context, cfg *config.DfstoreConfig, bucketName, objectKey, filepath string) error {
	start := time.Now()
	f, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	size := fi.Size()
	bar := progressbar.NewOptions64(
		size,
		progressbar.OptionShowBytes(true),
		progressbar.OptionUseANSICodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetDescription("[cyan]Uploading...[reset]"),
		progressbar.OptionSetRenderBlankState(true),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	tr := io.TeeReader(f, bar)
	if err := dfstore.New(cfg.Endpoint).PutObjectWithContext(ctx, &dfstore.PutObjectInput{
		BucketName:  bucketName,
		ObjectKey:   objectKey,
		Filter:      cfg.Filter,
		Mode:        cfg.Mode,
		MaxReplicas: cfg.MaxReplicas,
		Reader:      tr,
	}); err != nil {
		return err
	}

	fmt.Printf("upload object storage success, length: %d bytes cost: %d ms", size, time.Since(start).Milliseconds())
	return nil
}
