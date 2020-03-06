package localManager

import (
	"github.com/go-check/check"
	"testing"
)

type RequestManagerSuite struct{}

func Test(t *testing.T) {
	check.TestingT(t)
}

func init()  {
	check.Suite(&RequestManagerSuite{})
}

func (suite *RequestManagerSuite) TestRequestManager(c *check.C)  {
	for _, i := range []struct {
		name string
		urls []string
		expectUrls []string
		recentCount int
	}{
		{
			name: "oneUrlWithOneExpect",
			urls: []string{
				"a.com",
			},
			expectUrls: []string{
				"a.com",
			},
			recentCount: 1,
		},{
			name: "MoreUrlWithOneExpect",
			urls: []string{
				"a.com",
				"b.com",
				"c.com",
				"b.com",
			},
			expectUrls: []string{
				"b.com",
			},
			recentCount: 1,
		},
		{
			name: "MoreUrlWithTwoExpect",
			urls: []string{
				"a.com",
				"b.com",
				"c.com",
				"b.com",
				"d.com",
				"a.com",
				"e.com",
			},
			expectUrls: []string{
				"a.com",
				"e.com",
			},
			recentCount: 2,
		},
		{
			name: "twoUrlWithMoreExpect",
			urls: []string{
				"a.com",
				"b.com",
				"a.com",
			},
			expectUrls: []string{
				"a.com",
				"b.com",
			},
			recentCount: 5,
		},
		{
			name: "MoreUrlWithMoreExpect",
			urls: []string{
				"a.com",
				"b.com",
				"a.com",
				"c.com",
				"d.com",
				"e.com",
				"f.com",
				"f.com",
				"d.com",
			},
			expectUrls: []string{
				"a.com",
				"c.com",
				"e.com",
				"f.com",
				"d.com",
			},
			recentCount: 5,
		},
	}{
		c.Logf("testcase %s", i.name)
		rm := newRequestManager()
		for _, url := range i.urls {
			err := rm.addRequest(url, false)
			c.Assert(err, check.IsNil)
		}

		result := rm.getRecentRequest(i.recentCount)
		c.Assert(len(result), check.Equals, len(i.expectUrls))

		assertMap := map[string]struct{}{}
		for _, u := range result {
			assertMap[u] = struct{}{}
		}

		for _, epU := range i.expectUrls {
			_, exist := assertMap[epU]
			c.Assert(exist, check.Equals, true)
			delete(assertMap, epU)
		}

		c.Assert(len(assertMap), check.Equals, 0)
	}
}