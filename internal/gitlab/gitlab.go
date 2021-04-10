package gitlab

import (
	"context"
	"fmt"

	"github.com/machinebox/graphql"

	"github.com/bigredeye/concurrency_watcher/internal/types"
)

type Client struct {
	client *graphql.Client
	token  string
}

func NewClient(url string, token string) (*Client, error) {
	return &Client{
		client: graphql.NewClient(fmt.Sprintf("%s/api/graphql", url)),
		token:  token,
	}, nil
}

type GroupRes struct {
	Group types.Group `json:"group"`
}

func (c *Client) ListGroupRequests(groupPath string) (*types.Group, error) {
	req := graphql.NewRequest(`query($groupPath: ID!, $labels: [String!], $cursor: String!) {
  group(fullPath: $groupPath) {
    id
    name
    mergeRequests(labels: $labels, first: 100, sort: created_desc, after: $cursor) {
      count
      nodes {
        title
        author {
          name
          username
        }
        createdAt
        mergeStatus
        approvedBy {
          nodes {
            username
          }
        }
        headPipeline {
          status
        }
        webUrl
      }
      pageInfo {
        endCursor
        hasNextPage
      }
    }
  }
}`)

	req.Var("groupPath", groupPath)
	req.Var("labels", "hse")
	req.Var("cursor", "")

	req.Header.Set("Authorization", fmt.Sprint("Bearer ", c.token))

	var group GroupRes
	for {
		ctx := context.Background()

		var res GroupRes
		if err := c.client.Run(ctx, req, &res); err != nil {
			return nil, err
		}

		if group.Group.Id == "" {
			group.Group = res.Group
		} else {
			group.Group.MergeRequests.Nodes = append(group.Group.MergeRequests.Nodes, res.Group.MergeRequests.Nodes...)
		}

		if res.Group.MergeRequests.PageInfo.HasNextPage {
			req.Var("cursor", res.Group.MergeRequests.PageInfo.EndCursor)
		} else {
			break
		}
	}

	return &group.Group, nil
}
