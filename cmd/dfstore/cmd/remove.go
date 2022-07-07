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

	"github.com/spf13/cobra"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/dfstore"
)

var removeDescription = "remove object from P2P storage system."

// removeCmd represents the object storage remove command.
var removeCmd = &cobra.Command{
	Use:                "rm <target> [flags]",
	Short:              removeDescription,
	Long:               removeDescription,
	Args:               cobra.ExactArgs(1),
	DisableAutoGenTag:  true,
	SilenceUsage:       true,
	FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := cfg.Validate(); err != nil {
			return err
		}

		if err := validateRemoveArgs(args); err != nil {
			return err
		}

		bucketName, objectKey, err := parseDfstoreURL(args[0])
		if err != nil {
			return err
		}

		return runRemove(ctx, cfg, bucketName, objectKey)
	},
}

// Validate remove arguments.
func validateRemoveArgs(args []string) error {
	if !isDfstoreURL(args[0]) {
		return errors.New("invalid url, e.g. d7y://bucket_name/object_key")
	}

	return nil
}

// Remove object in bucket.
func runRemove(ctx context.Context, cfg *config.DfstoreConfig, bucketName, objectKey string) error {
	if err := dfstore.New(cfg.Endpoint).DeleteObjectWithContext(ctx, &dfstore.DeleteObjectInput{
		BucketName: bucketName,
		ObjectKey:  objectKey,
	}); err != nil {
		return fmt.Errorf("failed to remove %s in bucket %s: %w", bucketName, objectKey, err)
	}

	fmt.Printf("remove %s in bucket %s\n", bucketName, objectKey)
	return nil
}
