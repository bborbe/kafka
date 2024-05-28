// Copyright (c) 2024 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package badgerkv

import (
	libkv "github.com/bborbe/kv"
)

const bucketKeySeperator = byte('_')
const bucketKeySeperatorPlusone = byte('_' + 1)

func BucketToPrefix(bucket libkv.BucketName) []byte {
	return append(bucket, bucketKeySeperator)
}

func BucketAddKey(bucket libkv.BucketName, key []byte) []byte {
	return append(BucketToPrefix(bucket), key...)
}

func BucketRemoveKey(bucket libkv.BucketName, key []byte) []byte {
	return key[len(bucket)+1:]
}
