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

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
)

var (
	offset = flag.Int64("offset", 0, "")
	length = flag.Int64("length", -1, "")
	file   = flag.String("file", "", "")
)

func main() {
	flag.Parse()

	if len(*file) == 0 {
		os.Exit(1)
	}

	f, err := os.Open(*file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if *offset > 0 {
		_, err := f.Seek(*offset, io.SeekStart)
		if err != nil {
			panic(err)
		}
	}

	var rd io.Reader = f
	if *length > -1 {
		rd = io.LimitReader(f, *length)
	}

	hash := sha256.New()
	n, err := io.Copy(hash, rd)
	if err != nil {
		panic(err)
	}

	if *length > -1 && n != *length {
		panic(io.ErrShortWrite)
	}
	fmt.Printf("%s  %s", hex.EncodeToString(hash.Sum(nil)), *file)
}
