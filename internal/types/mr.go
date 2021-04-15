package types

type Group struct {
	Id            string                  `json:"id"`
	Name          string                  `json:"name"`
	MergeRequests MergeRequestsCollection `json:"mergeRequests"`
}

type MergeRequestsCollection struct {
	Count    int             `json:"count"`
	Nodes    []*MergeRequest `json:"nodes"`
	PageInfo Pagination      `json:"pageInfo"`
}

type Pagination struct {
	EndCursor   string `json:"endCursor"`
	HasNextPage bool   `json:"hasNextPage"`
}

type MergeRequest struct {
	Title        string               `json:"title"`
	Author       User                 `json:"author"`
	CreatedAt    string               `json:"createdAt"`
	MergeStatus  string               `json:"mergeStatus"`
	ApprovedBy   UserCollection       `json:"approvedBy"`
	HeadPipeline Pipeline             `json:"headPipeline"`
	WebUrl       string               `json:"webUrl"`
	Discussions  DiscussionCollection `json:"discussions"`
}

type User struct {
	Name     string `json:"name"`
	Username string `json:"username"`
}

type UserCollection struct {
	Nodes []*User `json:"nodes"`
}

type Pipeline struct {
	Status string `json:"status"`
}

type DiscussionCollection struct {
	Nodes []*Discussion
}

type Discussion struct {
	Resolved   bool
	Resolvable bool
}
