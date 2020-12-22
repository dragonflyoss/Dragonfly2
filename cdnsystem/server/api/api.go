/*
 *     Copyright 2020 The Dragonfly Authors
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

package api

var (
	Core   = newCategory("core api", "/api")

	System = newCategory("system api", "/sys")
)

var (
	apiCategories = make(map[string]*category)
)

func newCategory(name, prefix string) *category {
	if name == "" {
		return nil
	}
	if c, ok := apiCategories[name]; ok && c != nil {
		return c
	}

	apiCategories[name] = &category{
		name:   name,
		prefix: prefix,
		handlerSpecs: []*HandlerSpec{
			listHandler(name),
		},
	}
	return apiCategories[name]
}

// category groups the APIs.
type category struct {
	name         string
	prefix       string
	handlerSpecs []*HandlerSpec
}

// Register registers an API into this API category.
func (c *category) Register(handlers ...*HandlerSpec) *category {
	for _, h := range handlers {
		if valid(h) {
			c.handlerSpecs = append(c.handlerSpecs, h)
		}
	}
	return c
}

// Name returns the name of this category.
func (c *category) Name() string {
	return c.name
}

// Prefix returns the api prefix of this category.
func (c *category) Prefix() string {
	return c.prefix
}

// Handlers returns all of the APIs registered into this category.
func (c *category) Handlers() []*HandlerSpec {
	return c.handlerSpecs
}

// Range traverses all the handlers in this category.
func (c *category) Range(f func(prefix string, h *HandlerSpec)) {
	for _, h := range c.handlerSpecs {
		if h != nil {
			f(c.prefix, h)
		}
	}
}
