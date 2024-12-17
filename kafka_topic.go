// Copyright (c) 2023 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kafka

import (
	"context"
	"regexp"
	"strings"

	"github.com/bborbe/errors"
	"github.com/bborbe/validation"
)

var invalidTopicCharRegexp = regexp.MustCompile(`[^a-zA-Z0-9\._-]+`)
var dashRegexp = regexp.MustCompile(`-+`)
var validateTopic = regexp.MustCompile(`^[a-zA-Z0-9\._-]*$`)

func TopicFromStrings(values ...string) Topic {
	str := strings.ToLower(strings.Join(values, "-"))
	str = invalidTopicCharRegexp.ReplaceAllString(str, "-")
	str = dashRegexp.ReplaceAllString(str, "-")
	str = strings.TrimSuffix(str, "-")
	return Topic(str)
}

type Topic string

func (t Topic) String() string {
	return string(t)
}

func (t Topic) Validate(ctx context.Context) error {
	if len(t) == 0 {
		return errors.Wrapf(ctx, validation.Error, "Topic empty")
	}
	if !validateTopic.MatchString(t.String()) {
		return errors.Wrap(ctx, validation.Error, "topic has invalid character")
	}
	return nil
}
